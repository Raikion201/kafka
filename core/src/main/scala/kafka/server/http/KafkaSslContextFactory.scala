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

import java.net.InetSocketAddress
import java.util

import javax.net.ssl.{SSLContext, SSLEngine}

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.{ListenerName, ListenerReconfigurable}
import org.apache.kafka.common.security.ssl.SslFactory

import org.eclipse.jetty.util.ssl.SslContextFactory

/**
 * Bridges Jetty's [[SslContextFactory.Server]] to Kafka's [[SslFactory]].
 *
 * Every `SSLEngine` Jetty needs for an incoming TLS connection is produced by
 * `SslFactory` — so the HTTPS REST listener goes through exactly the same
 * pipeline as a Kafka `SSL://` listener:
 *
 *   - the `ssl.engine.factory.class` plugin (default `DefaultSslEngineFactory`)
 *     is honoured, so HSM-backed keys or audit-wrapped engines apply to HTTPS
 *     just like they do to SSL://
 *   - certificate rotation via `kafka-configs.sh --alter ssl.keystore.location`
 *     flows through `SslFactory.reconfigure` which runs
 *     `CertificateEntries.ensureCompatible` + `SslEngineValidator.validate`
 *     before swapping the engine factory
 *   - per-listener overrides keyed on `listener.name.<name>.ssl.*` take effect
 *     because we feed `SslFactory` configs pulled with
 *     `valuesWithPrefixOverride(listenerName.configPrefix)`
 *
 * Implements [[ListenerReconfigurable]] so `DynamicBrokerConfig` invokes
 * `validateReconfiguration`/`reconfigure` with the correct listener-scoped
 * config snapshot.
 *
 * @param listener  listener name for this HTTPS endpoint (e.g. `HTTPS`);
 *                  also the key used to look up per-listener SSL overrides.
 * @param sslFactory an already-configured [[SslFactory]] in
 *                   [[ConnectionMode.SERVER]]. This class takes ownership of
 *                   its lifecycle — callers must not close it themselves.
 */
class KafkaSslContextFactory(listener: ListenerName, sslFactory: SslFactory)
    extends SslContextFactory.Server with ListenerReconfigurable {

  // Jetty's SslContextFactory lifecycle validates that an SSLContext is
  // available before the server starts. We will never delegate engine
  // creation to that context — newSSLEngine below is overridden — but we
  // still need doStart() to succeed. Hand Jetty the JVM-wide default context
  // purely to pass its precondition check.
  setSslContext(SSLContext.getDefault)

  override def listenerName(): ListenerName = listener

  override def reconfigurableConfigs(): util.Set[String] =
    sslFactory.reconfigurableConfigs()

  override def validateReconfiguration(configs: util.Map[String, _]): Unit =
    sslFactory.validateReconfiguration(configs)

  override def reconfigure(configs: util.Map[String, _]): Unit =
    sslFactory.reconfigure(configs)

  override def configure(configs: util.Map[String, _]): Unit =
    throw new ConfigException(
      "KafkaSslContextFactory is configured indirectly via SslFactory at construction time")

  override def newSSLEngine(): SSLEngine =
    sslFactory.createSslEngine(null, 0)

  override def newSSLEngine(address: InetSocketAddress): SSLEngine = {
    // Jetty occasionally passes null here (request has no remote address).
    val host = if (address == null) null else address.getHostString
    val port = if (address == null) 0 else address.getPort
    sslFactory.createSslEngine(host, port)
  }

  override def doStop(): Unit = {
    try sslFactory.close()
    finally super.doStop()
  }
}
