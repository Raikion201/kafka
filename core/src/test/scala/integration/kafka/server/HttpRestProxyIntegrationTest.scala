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

/**
 * Integration tests for the embedded HTTP REST proxy (`POST /v1/topics/{name}`).
 *
 * Extends [[IntegrationTestHarness]], which is Kafka's standard base for
 * in-JVM integration tests. Before each test it boots a real KRaft controller
 * plus one broker inside the test JVM — same code paths as production, just
 * wired up to ephemeral ports — and tears them down after. That's why we use
 * it rather than mocks: the thing we want to verify is the wiring between
 * Jetty, Jersey, the Basic Auth filter, and the broker's real produce path,
 * and a mock would paper over exactly the bugs we care about.
 *
 * The harness gives us `createTopic`, `createConsumer`, `brokerCount`,
 * `modifyConfigs`, etc. We override `modifyConfigs` to append an `HTTP://`
 * listener and the basic-auth credentials to the broker properties before
 * startup, so the REST server comes up on a known port alongside the normal
 * binary Kafka listener.
 */
class HttpRestProxyIntegrationTest extends IntegrationTestHarness {

  // One broker is enough — these tests don't exercise replication.
  override def brokerCount: Int = 1

  private val httpUser = "alice"
  private val httpPass = "s3cret"
  private val httpCreds = Base64.getEncoder.encodeToString(s"$httpUser:$httpPass".getBytes(UTF_8))

  // We need to know the HTTP port up front so we can both put it in the
  // broker config and build URLs from the test. Open a ServerSocket on port
  // 0 to have the OS pick a free one, record it, and immediately close the
  // socket so Jetty can bind to it when the broker starts. There is a small
  // TOCTOU window where another process could grab the port between close
  // and bind; acceptable for an integration test.
  private val httpPort: Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()
  }

  /**
   * Hook called by the harness just before each broker is constructed.
   * We append our HTTP listener to the `listeners=` line the harness
   * already built (which contains the normal PLAINTEXT listener for
   * binary Kafka clients), and set the basic-auth credentials.
   */
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

  /** Fire an HTTP POST at the embedded REST server. */
  private def post(path: String, body: String, authHeader: Option[String] = Some("Basic " + httpCreds)):
      HttpResponse[String] = {
    val builder = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$httpPort$path"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
    authHeader.foreach(h => builder.header("Authorization", h))
    httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
  }

  /** Fire an HTTP GET at the embedded REST server. */
  private def get(path: String, authHeader: Option[String] = Some("Basic " + httpCreds)):
      HttpResponse[String] = {
    val builder = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$httpPort$path"))
      .GET()
    authHeader.foreach(h => builder.header("Authorization", h))
    httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
  }

  /** Fire an HTTP DELETE at the embedded REST server. */
  private def delete(path: String, authHeader: Option[String] = Some("Basic " + httpCreds)):
      HttpResponse[String] = {
    val builder = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$httpPort$path"))
      .DELETE()
    authHeader.foreach(h => builder.header("Authorization", h))
    httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
  }

  /** Escape a schema string so it can be embedded as a JSON string value. */
  private def schemaBody(schema: String): String =
    s"""{"schema":"${schema.replace("\"", "\\\"")}"}"""

  /**
   * Happy path — the load-bearing test. We POST a record over HTTP, then
   * spin up a normal `KafkaConsumer` against the same broker and read the
   * exact bytes back from the exact partition and offset the HTTP response
   * claimed. If that works, every hop in the chain is proven live:
   *
   *   Jetty bound the port →
   *   BasicAuthFilter accepted the credentials →
   *   Jersey dispatched to `ProduceResource.produce` →
   *   `AuthHelper.authorize` passed →
   *   `MetadataCache.getTopicId` / `numPartitions` worked →
   *   `MemoryRecords` was built correctly →
   *   `ReplicaManager.appendRecords` actually appended →
   *   The `CompletableFuture` callback returned a real offset →
   *   The record is durable and readable by a normal Kafka client.
   */
  @Test
  def httpProducedRecordReachesTopic(): Unit = {
    val topic = "rest-happy"
    createTopic(topic, numPartitions = 3, replicationFactor = 1)

    // Produce over HTTP. Expect 200 + {partition, offset} in the JSON body.
    val resp = post(s"/v1/topics/$topic", """{"key":"k1","value":"hello-from-http"}""")
    assertEquals(200, resp.statusCode(), s"Body: ${resp.body()}")

    val json = mapper.readTree(resp.body())
    val partition = json.get("partition").asInt()
    val offset = json.get("offset").asLong()
    assertTrue(partition >= 0 && partition < 3, s"partition out of range: $partition")
    assertEquals(0L, offset)

    // IntegrationTestHarness requires group.protocol to be set on the
    // harness-level consumerConfig (not just overrides). Set "classic"
    // because we're not testing the new consumer rebalance protocol here.
    consumerConfig.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic")

    val consumerProps = new Properties()
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rest-proxy-test")
    val consumer = createConsumer(new StringDeserializer, new StringDeserializer, consumerProps)
    try {
      consumer.subscribe(java.util.List.of(topic))

      // Poll up to 10s for the record the HTTP request just produced.
      val deadline = System.currentTimeMillis() + 10000
      var received: List[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]] = Nil
      while (received.isEmpty && System.currentTimeMillis() < deadline) {
        received = consumer.poll(Duration.ofMillis(500)).asScala.toList
      }

      // The record we read back must match what we POSTed, byte-for-byte,
      // on the same partition and offset the HTTP response announced.
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

  /**
   * No `Authorization` header → BasicAuthFilter must reject with 401 and
   * the spec-required `WWW-Authenticate` header so clients know to retry
   * with Basic credentials.
   */
  @Test
  def missingAuthReturns401(): Unit = {
    val resp = post("/v1/topics/anything", """{"key":"k","value":"v"}""", authHeader = None)
    assertEquals(401, resp.statusCode())
    assertTrue(resp.headers().firstValue("WWW-Authenticate").isPresent,
      "401 response missing WWW-Authenticate header")
  }

  /**
   * Wrong password → 401. Proves the filter actually compares against the
   * configured credentials rather than letting any Basic-shaped header pass.
   */
  @Test
  def wrongPasswordReturns401(): Unit = {
    val bad = Base64.getEncoder.encodeToString("alice:wrong".getBytes(UTF_8))
    val resp = post("/v1/topics/anything", """{"key":"k","value":"v"}""",
      authHeader = Some("Basic " + bad))
    assertEquals(401, resp.statusCode())
  }

  /**
   * Topic that does not exist in metadata → 404. Exercises the
   * `metadataCache.getTopicId` branch in `ProduceResource` that turns
   * `Uuid.ZERO_UUID` (unknown topic) into an HTTP 404 instead of letting
   * the request fall through to ReplicaManager and fail obscurely.
   */
  @Test
  def unknownTopicReturns404(): Unit = {
    val resp = post("/v1/topics/this-topic-does-not-exist", """{"key":"k","value":"v"}""")
    assertEquals(404, resp.statusCode())
  }

  // ── Schema Registry integration tests ─────────────────────────────────────

  private val schemaA = """{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"""
  private val schemaB = """{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"""

  /**
   * Register a schema, then fetch it back by ID.
   * Proves the full round-trip: POST assigns an ID, GET /v1/schemas/{id}
   * returns the same schema content with correct subject+version metadata.
   */
  @Test
  def schemaRegistryRegisterAndFetchById(): Unit = {
    val subject = "user-value"

    // Register
    val regResp = post(s"/v1/schemas/subjects/$subject", schemaBody(schemaA))
    assertEquals(201, regResp.statusCode(), s"register body: ${regResp.body()}")
    val regJson = mapper.readTree(regResp.body())
    val id = regJson.get("id").asInt()
    assertEquals(1, id)

    // Fetch by ID
    val fetchResp = get(s"/v1/schemas/$id")
    assertEquals(200, fetchResp.statusCode(), s"fetch body: ${fetchResp.body()}")
    val fetchJson = mapper.readTree(fetchResp.body())
    assertEquals(id, fetchJson.get("id").asInt())
    assertEquals(subject, fetchJson.get("subject").asText())
    assertEquals(1, fetchJson.get("version").asInt())
    assertEquals(schemaA, fetchJson.get("schema").asText())
  }

  /**
   * Registering the same schema twice under the same subject returns the
   * same ID (deduplication) without creating a second version.
   */
  @Test
  def schemaRegistryDedupReturnsSameId(): Unit = {
    val subject = "order-value"
    val r1 = post(s"/v1/schemas/subjects/$subject", schemaBody(schemaA))
    val r2 = post(s"/v1/schemas/subjects/$subject", schemaBody(schemaA))
    assertEquals(201, r1.statusCode())
    assertEquals(201, r2.statusCode())
    val id1 = mapper.readTree(r1.body()).get("id").asInt()
    val id2 = mapper.readTree(r2.body()).get("id").asInt()
    assertEquals(id1, id2, "same schema content must return same id")

    // Only one version exists
    val versResp = get(s"/v1/schemas/subjects/$subject")
    val versions = mapper.readTree(versResp.body()).get("versions")
    assertEquals(1, versions.size(), "dedup must not create a second version")
  }

  /**
   * Registering two different schemas under the same subject creates two
   * versions with different IDs, both retrievable.
   */
  @Test
  def schemaRegistryTwoVersions(): Unit = {
    val subject = "item-value"
    val r1 = post(s"/v1/schemas/subjects/$subject", schemaBody(schemaA))
    val r2 = post(s"/v1/schemas/subjects/$subject", schemaBody(schemaB))
    assertEquals(201, r1.statusCode())
    assertEquals(201, r2.statusCode())
    val id1 = mapper.readTree(r1.body()).get("id").asInt()
    val id2 = mapper.readTree(r2.body()).get("id").asInt()
    assertTrue(id1 != id2, "different schemas must get different ids")

    // Version list contains both
    val versResp = get(s"/v1/schemas/subjects/$subject")
    assertEquals(200, versResp.statusCode())
    val versions = mapper.readTree(versResp.body()).get("versions")
    assertEquals(2, versions.size())

    // Fetch version 1 and version 2 individually
    val v1Resp = get(s"/v1/schemas/subjects/$subject/versions/1")
    assertEquals(200, v1Resp.statusCode())
    assertEquals(schemaA, mapper.readTree(v1Resp.body()).get("schema").asText())

    val v2Resp = get(s"/v1/schemas/subjects/$subject/versions/2")
    assertEquals(200, v2Resp.statusCode())
    assertEquals(schemaB, mapper.readTree(v2Resp.body()).get("schema").asText())
  }

  /**
   * DELETE removes the subject; subsequent GET returns 404.
   */
  @Test
  def schemaRegistryDeleteSubject(): Unit = {
    val subject = "temp-value"
    post(s"/v1/schemas/subjects/$subject", schemaBody(schemaA))

    val delResp = delete(s"/v1/schemas/subjects/$subject")
    assertEquals(200, delResp.statusCode(), s"delete body: ${delResp.body()}")
    val delJson = mapper.readTree(delResp.body())
    assertEquals(subject, delJson.get("subject").asText())
    assertEquals(1, delJson.get("versions").size())

    // Subject is gone
    val afterResp = get(s"/v1/schemas/subjects/$subject")
    assertEquals(404, afterResp.statusCode())
  }

  /** GET on unknown schema ID → 404. */
  @Test
  def schemaRegistryUnknownIdReturns404(): Unit = {
    val resp = get("/v1/schemas/999")
    assertEquals(404, resp.statusCode())
    assertEquals("SCHEMA_NOT_FOUND", mapper.readTree(resp.body()).get("error").asText())
  }

  /** GET versions on unknown subject → 404. */
  @Test
  def schemaRegistryUnknownSubjectReturns404(): Unit = {
    val resp = get("/v1/schemas/subjects/no-such-subject")
    assertEquals(404, resp.statusCode())
    assertEquals("SUBJECT_NOT_FOUND", mapper.readTree(resp.body()).get("error").asText())
  }

  /** POST with missing schema field → 400. */
  @Test
  def schemaRegistryMissingSchemaFieldReturns400(): Unit = {
    val resp = post("/v1/schemas/subjects/bad-value", """{"other":"field"}""")
    assertEquals(400, resp.statusCode())
    assertEquals("SCHEMA_MISSING", mapper.readTree(resp.body()).get("error").asText())
  }

  /** Schema registry endpoints respect Basic Auth — no credentials → 401. */
  @Test
  def schemaRegistryRequiresAuth(): Unit = {
    val resp = post("/v1/schemas/subjects/user-value", schemaBody(schemaA), authHeader = None)
    assertEquals(401, resp.statusCode())
  }
}
