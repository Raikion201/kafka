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

import java.util.Properties

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.{KRaftConfigs, QuorumConfig}
import org.apache.kafka.server.config.{HttpServerConfigs, ReplicationConfigs, ServerLogConfigs}

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import kafka.server.KafkaConfig

/**
 * Unit tests for the HTTP-REST-proxy-specific accessors on KafkaConfig.
 * The integration test covers the live server; these cover the pure
 * config parsing paths that would otherwise only be exercised indirectly.
 */
class HttpConfigTest {

  /** Build minimum viable KRaft broker props plus any overrides. */
  private def props(overrides: (String, String)*): Properties = {
    val p = new Properties()
    p.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    // Node id must NOT appear in voters when process.roles is just "broker".
    p.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    p.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    p.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093")
    p.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
    p.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
    p.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/kafka-logs")
    overrides.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }

  @Test
  def pureHttpListenerIsRoutedToHttpListeners(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092,HTTP://0.0.0.0:8080"))
    val http = cfg.httpListeners
    assertEquals(1, http.size)
    assertEquals("0.0.0.0", http.head.host)
    assertEquals(8080, http.head.port)
    assertFalse(http.head.isTls)
    // And the standard `listeners` accessor must have stripped HTTP out.
    val std = cfg.listeners
    assertEquals(1, std.size, s"standard listener parser should only see PLAINTEXT, got $std")
  }

  @Test
  def emptyHostDefaultsToWildcard(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092,HTTP://:8080"))
    assertEquals("0.0.0.0", cfg.httpListeners.head.host)
  }

  @Test
  def httpsListenerIsTaggedAsTls(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092,HTTPS://:8443"))
    assertTrue(cfg.httpListeners.head.isTls)
  }

  @Test
  def ipv6BracketFormIsAccepted(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092,HTTP://[::1]:8080"))
    val http = cfg.httpListeners
    assertEquals(1, http.size)
    assertEquals("::1", http.head.host, "brackets must be stripped for ServerConnector.setHost")
    assertEquals(8080, http.head.port)
  }

  @Test
  def missingHttpListenerIsEmptyNotNull(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092"))
    assertTrue(cfg.httpListeners.isEmpty)
  }

  @Test
  def basicCredentialsParsedIntoMap(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092",
      HttpServerConfigs.HTTP_REST_BASIC_CREDENTIALS_CONFIG -> "alice:s3cret,bob:hunter2"))
    val creds = cfg.httpBasicCredentials
    assertEquals(2, creds.size)
    assertEquals("s3cret", creds("alice"))
    assertEquals("hunter2", creds("bob"))
  }

  @Test
  def basicCredentialsEmptyByDefault(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092"))
    assertTrue(cfg.httpBasicCredentials.isEmpty)
  }

  @Test
  def malformedCredentialThrowsConfigException(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092",
      HttpServerConfigs.HTTP_REST_BASIC_CREDENTIALS_CONFIG -> "no-colon-here"))
    assertThrows(classOf[ConfigException], () => cfg.httpBasicCredentials)
  }

  @Test
  def defaultExecutorThreadsIs8(): Unit = {
    val cfg = new KafkaConfig(props(
      SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092"))
    assertEquals(8, cfg.httpExecutorThreads)
  }

  @Test
  def executorThreadsBelowFourIsRejectedAtConstruction(): Unit = {
    assertThrows(classOf[org.apache.kafka.common.config.ConfigException], () =>
      new KafkaConfig(props(
        SocketServerConfigs.LISTENERS_CONFIG -> "PLAINTEXT://:9092",
        HttpServerConfigs.HTTP_REST_EXECUTOR_THREADS_CONFIG -> "2")))
  }
}
