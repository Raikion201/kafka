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
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.http.SslContextFactories

import org.eclipse.jetty.ee10.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.servlet.ServletContainer
import org.slf4j.LoggerFactory

import kafka.server.{KafkaConfig, ReplicaManager}

/**
 * Embedded HTTP REST server for the Kafka broker. Activated when `listeners=`
 * contains one or more `HTTP://` or `HTTPS://` entries. Owns the Jetty
 * lifecycle; the routes and filters live in [[HttpRouter]].
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

  @volatile private var jetty: Server = _

  def startup(): Unit = {
    val pool = new QueuedThreadPool(executorThreads)
    pool.setName("http-rest")
    jetty = new Server(pool)

    // Reuses the broker's existing ssl.keystore.*/ssl.truststore.* configs —
    // no separate SSL keys for the REST server.
    val ssl = if (endpoints.exists(_.isTls))
      SslContextFactories.createServerSideSslContextFactory(brokerConfig)
    else null

    endpoints.foreach { ep =>
      val connector =
        if (ep.isTls) new ServerConnector(jetty, ssl)
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

  def shutdown(): Unit = {
    val server = jetty
    if (server == null) return
    try {
      server.stop()
      server.join()
    } finally {
      server.destroy()
      jetty = null
    }
  }
}
