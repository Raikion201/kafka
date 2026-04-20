/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.http

import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.network.{ConnectionMode, ListenerName}
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.authorizer.Authorizer

import org.eclipse.jetty.ee10.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.servlet.ServletContainer
import org.slf4j.LoggerFactory

import kafka.server.{KafkaConfig, ReplicaManager}

import scala.collection.mutable

/**
 * Embedded HTTP REST server for the Kafka broker. Activated when `listeners=`
 * contains one or more `HTTP://` or `HTTPS://` entries. Owns the Jetty
 * lifecycle; the routes and filters live in [[HttpRouter]].
 *
 * TLS goes through Kafka's [[SslFactory]] pipeline (see
 * [[KafkaSslContextFactory]]): the HTTPS listener honours
 * `ssl.engine.factory.class`, supports hot keystore rotation via
 * `DynamicBrokerConfig`, and picks up `listener.name.<name>.ssl.*`
 * per-listener overrides — the same behaviours Kafka's `SSL://` listeners have.
 */
class HttpRestServer(
    endpoints: Seq[KafkaConfig.HttpEndpoint],
    executorThreads: Int,
    basicCredentials: Map[String, String],
    brokerConfig: KafkaConfig,
    replicaManager: ReplicaManager,
    authorizerPlugin: Option[Plugin[Authorizer]],
    metadataCache: MetadataCache,
    time: Time) {

  require(endpoints.nonEmpty, "HttpRestServer requires at least one endpoint")

  private val log = LoggerFactory.getLogger(classOf[HttpRestServer])

  // Jetty needs a distinct SslContextFactory per HTTPS listener name; multiple
  // HTTPS endpoints sharing a listener name (the common case — all HTTPS
  // entries resolve to the listener name "HTTPS") share one KafkaSslContextFactory
  // and one SslFactory, so reconfig fires once per cert rotation.
  private val sslFactoriesByListener = mutable.LinkedHashMap.empty[ListenerName, KafkaSslContextFactory]

  @volatile private var jetty: Server = _

  def startup(): Unit = {
    val pool = new QueuedThreadPool(executorThreads)
    pool.setName("http-rest")
    jetty = new Server(pool)

    endpoints.foreach { ep =>
      val connector =
        if (ep.isTls) new ServerConnector(jetty, sslContextFactoryFor(ep))
        else new ServerConnector(jetty)
      connector.setHost(ep.host)
      connector.setPort(ep.port)
      jetty.addConnector(connector)
    }

    val context = new ServletContextHandler
    context.setContextPath("/")
    context.addServlet(
      new ServletHolder(new ServletContainer(
        HttpRouter.build(basicCredentials, replicaManager, authorizerPlugin, metadataCache, time))),
      "/*")
    jetty.setHandler(context)

    jetty.start()
    log.info("HTTP REST server listening on {}", endpoints)
  }

  /**
   * Build (or reuse) the [[KafkaSslContextFactory]] for the listener name this
   * endpoint resolves to. Each such factory wraps a fresh [[SslFactory]] that
   * we configure with `listener.name.<listener>.ssl.*` overrides layered on
   * top of the broker's global `ssl.*` configs, then register with
   * `DynamicBrokerConfig` so `kafka-configs.sh --alter` can rotate the cert
   * at runtime.
   */
  private def sslContextFactoryFor(ep: KafkaConfig.HttpEndpoint): SslContextFactory.Server = {
    val listener = listenerNameOf(ep)
    sslFactoriesByListener.getOrElseUpdate(listener, {
      val configs = brokerConfig.valuesWithPrefixOverride(listener.configPrefix)
      val sslFactory = new SslFactory(
        ConnectionMode.SERVER,
        /* clientAuthConfigOverride */ null,
        // REST clients are external — their certs aren't in the broker's
        // truststore, so we can't do a mock handshake against it at startup.
        // SslFactory still validates on every reconfigure via CertificateEntries
        // + SslEngineValidator; that's the rotation-time safety net.
        /* keystoreVerifiableUsingTruststore */ false)
      sslFactory.configure(configs)
      val jettyFactory = new KafkaSslContextFactory(listener, sslFactory)
      brokerConfig.addReconfigurable(jettyFactory)
      jettyFactory
    })
  }

  private def listenerNameOf(ep: KafkaConfig.HttpEndpoint): ListenerName =
    new ListenerName(if (ep.isTls) "HTTPS" else "HTTP")

  def shutdown(): Unit = {
    val server = jetty
    sslFactoriesByListener.values.foreach { factory =>
      try brokerConfig.removeReconfigurable(factory)
      catch { case e: Throwable => log.warn("Error removing reconfigurable for {}", factory.listenerName, e) }
    }
    sslFactoriesByListener.clear()

    if (server == null) return
    try {
      server.stop()   // triggers doStop() on each SslContextFactory, which closes the SslFactory
      server.join()
    } finally {
      server.destroy()
      jetty = null
    }
  }
}
