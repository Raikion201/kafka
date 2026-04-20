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
package kafka.server

import java.net.{ServerSocket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.{Base64, Properties}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.HttpServerConfigs

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import kafka.api.IntegrationTestHarness

class HttpRestProxyIntegrationTest extends IntegrationTestHarness {

  override def brokerCount: Int = 1

  private val httpUser = "alice"
  private val httpPass = "s3cret"
  private val httpCreds = Base64.getEncoder.encodeToString(s"$httpUser:$httpPass".getBytes(UTF_8))

  // Pre-allocate a port so we can put it in the broker config and use it in URLs.
  // Small TOCTOU risk; acceptable for an integration test.
  private val httpPort: Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()
  }

  override def modifyConfigs(props: scala.collection.Seq[Properties]): Unit = {
    super.modifyConfigs(props)
    props.foreach { p =>
      val existing = p.getProperty(SocketServerConfigs.LISTENERS_CONFIG)
      p.setProperty(SocketServerConfigs.LISTENERS_CONFIG, s"$existing,HTTP://127.0.0.1:$httpPort")
      p.setProperty(HttpServerConfigs.HTTP_REST_BASIC_CREDENTIALS_CONFIG, s"$httpUser:$httpPass")
    }
  }

  private val httpClient = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()

  private def post(path: String, body: String, authHeader: Option[String] = Some("Basic " + httpCreds)):
      HttpResponse[String] = {
    val builder = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$httpPort$path"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
    authHeader.foreach(h => builder.header("Authorization", h))
    httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
  }

  @Test
  def httpProducedRecordReachesTopic(): Unit = {
    val topic = "rest-happy"
    createTopic(topic, numPartitions = 3, replicationFactor = 1)

    val resp = post(s"/v1/topics/$topic", """{"key":"k1","value":"hello-from-http"}""")
    assertEquals(200, resp.statusCode(), s"Body: ${resp.body()}")

    val json = mapper.readTree(resp.body())
    val partition = json.get("partition").asInt()
    val offset = json.get("offset").asLong()
    assertTrue(partition >= 0 && partition < 3, s"partition out of range: $partition")
    assertEquals(0L, offset)

    consumerConfig.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic")
    val consumerProps = new Properties()
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rest-proxy-test")
    val consumer = createConsumer(new StringDeserializer, new StringDeserializer, consumerProps)
    try {
      consumer.subscribe(java.util.List.of(topic))

      val deadline = System.currentTimeMillis() + 10000
      var received: List[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]] = Nil
      while (received.isEmpty && System.currentTimeMillis() < deadline) {
        received = consumer.poll(Duration.ofMillis(500)).asScala.toList
      }

      assertEquals(1, received.size, "expected exactly one record")
      val record = received.head
      assertEquals("k1", record.key())
      assertEquals("hello-from-http", record.value())
      assertEquals(partition, record.partition())
      assertEquals(offset, record.offset())
    } finally {
      consumer.close()
    }
  }

  @Test
  def missingAuthReturns401(): Unit = {
    val resp = post("/v1/topics/anything", """{"key":"k","value":"v"}""", authHeader = None)
    assertEquals(401, resp.statusCode())
    assertTrue(resp.headers().firstValue("WWW-Authenticate").isPresent,
      "401 response missing WWW-Authenticate header")
  }

  @Test
  def wrongPasswordReturns401(): Unit = {
    val bad = Base64.getEncoder.encodeToString("alice:wrong".getBytes(UTF_8))
    val resp = post("/v1/topics/anything", """{"key":"k","value":"v"}""",
      authHeader = Some("Basic " + bad))
    assertEquals(401, resp.statusCode())
  }

  @Test
  def unknownTopicReturns404(): Unit = {
    val resp = post("/v1/topics/this-topic-does-not-exist", """{"key":"k","value":"v"}""")
    assertEquals(404, resp.statusCode())
  }
}
