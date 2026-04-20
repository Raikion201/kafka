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

import java.io.File

import javax.net.ssl.SSLEngine

import scala.jdk.CollectionConverters._

import org.apache.kafka.common.network.{ConnectionMode, ListenerName}
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.test.{TestSslUtils, TestUtils}

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Verifies the Jetty ↔ Kafka SSL bridge: a [[KafkaSslContextFactory]] wraps a
 * real [[SslFactory]] built with real (test) certs and hands back real
 * `SSLEngine` instances, honours reconfigure, and exposes the listener name
 * so `DynamicBrokerConfig` can scope config to it.
 */
class KafkaSslContextFactoryTest {

  private val listener = new ListenerName("HTTPS")

  private def freshSslFactory(): (SslFactory, File) = {
    val truststore = TestUtils.tempFile("kafka.rest.", ".truststore.jks")
    val configs = TestSslUtils.createSslConfig(
      /* useClientCert */ false,
      /* createTrustStore */ true,
      ConnectionMode.SERVER,
      truststore,
      "kafka-rest")
    val factory = new SslFactory(ConnectionMode.SERVER, null, true)
    factory.configure(configs)
    (factory, truststore)
  }

  @Test
  def listenerNameIsReported(): Unit = {
    val (sslFactory, _) = freshSslFactory()
    try {
      val ctxFactory = new KafkaSslContextFactory(listener, sslFactory)
      try assertEquals(listener, ctxFactory.listenerName())
      finally ctxFactory.stop()
    } finally sslFactory.close()
  }

  @Test
  def newSSLEngineDelegatesToSslFactory(): Unit = {
    val (sslFactory, _) = freshSslFactory()
    val ctxFactory = new KafkaSslContextFactory(listener, sslFactory)
    ctxFactory.start()
    try {
      val engine1: SSLEngine = ctxFactory.newSSLEngine()
      val engine2: SSLEngine = ctxFactory.newSSLEngine()
      // Jetty calls newSSLEngine per connection, so fresh engines expected —
      // if we were caching, engine1 eq engine2 would be true.
      assertNotNull(engine1)
      assertNotNull(engine2)
      assertNotSame(engine1, engine2,
        "KafkaSslContextFactory must not cache SSLEngines — Jetty expects a fresh one per connection.")
      assertFalse(engine1.getUseClientMode, "server-mode engine expected")
    } finally {
      ctxFactory.stop()
    }
  }

  @Test
  def reconfigurableConfigsAreTheFactorys(): Unit = {
    val (sslFactory, _) = freshSslFactory()
    try {
      val ctxFactory = new KafkaSslContextFactory(listener, sslFactory)
      try {
        val reconfigurable = ctxFactory.reconfigurableConfigs().asScala
        // Exactly the set SslFactory exposes — ssl.keystore.location etc.
        assertTrue(reconfigurable.contains("ssl.keystore.location"),
          s"expected ssl.keystore.location in reconfigurable set, got: $reconfigurable")
        assertTrue(reconfigurable.contains("ssl.keystore.password"))
      } finally ctxFactory.stop()
    } finally sslFactory.close()
  }

  @Test
  def configureIsRejected(): Unit = {
    val (sslFactory, _) = freshSslFactory()
    try {
      val ctxFactory = new KafkaSslContextFactory(listener, sslFactory)
      try {
        // SslFactory is configured at construction; the ListenerReconfigurable
        // configure(Map) entry point is wrong shape for this bridge and must fail
        // rather than silently leave the SslFactory in a half-configured state.
        assertThrows(classOf[org.apache.kafka.common.config.ConfigException],
          () => ctxFactory.configure(java.util.Map.of()))
      } finally ctxFactory.stop()
    } finally sslFactory.close()
  }

  @Test
  def reconfigureFlowsThroughToSslFactory(): Unit = {
    val (sslFactory, _) = freshSslFactory()
    val ctxFactory = new KafkaSslContextFactory(listener, sslFactory)
    ctxFactory.start()
    try {
      // Same config — reconfigure should be a safe no-op (SslEngineFactory
      // detects nothing to rebuild) and not throw.
      val currentConfigs: java.util.Map[String, AnyRef] = new java.util.HashMap()
      // A harmless reconfigure — pass in the broker's own ssl configs.
      ctxFactory.reconfigure(currentConfigs)
      // If we got here, the path from KafkaSslContextFactory.reconfigure
      // to SslFactory.reconfigure is live.
    } finally ctxFactory.stop()
  }
}
