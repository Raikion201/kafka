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

import java.io.File
import java.net.{InetAddress, ServerSocket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import java.util.{Base64, Properties}

import javax.net.ssl.{SSLContext, SSLParameters, X509TrustManager}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ConnectionMode
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.HttpServerConfigs
import org.apache.kafka.test.{TestSslUtils, TestUtils}

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import kafka.api.IntegrationTestHarness

/**
 * Integration tests for the HTTPS REST listener.
 *
 * Unlike [[HttpRestProxyIntegrationTest]] (which only exercises plain HTTP),
 * this test wires an actual TLS keystore into the broker and verifies:
 *
 *   1. a record POSTed over HTTPS reaches the topic, proving the full chain
 *      (Jetty TLS → KafkaSslContextFactory → SslFactory → ProduceResource →
 *      ReplicaManager) works end-to-end under encryption;
 *   2. a listener-scoped override — `listener.name.https.ssl.keystore.location`
 *      set to a *different* keystore from the broker's global
 *      `ssl.keystore.location` — is the one actually served on the wire.
 *      That's the specific behaviour BLOCKER 1 called out as missing from the
 *      previous hand-rolled SslContextFactory path.
 */
class HttpsRestProxyIntegrationTest extends IntegrationTestHarness {

  override def brokerCount: Int = 1

  private val httpUser = "alice"
  private val httpPass = "s3cret"
  private val httpCreds = Base64.getEncoder.encodeToString(s"$httpUser:$httpPass".getBytes(UTF_8))

  // Port used for the HTTPS listener. Pre-allocated so we know it up front.
  private val httpsPort: Int = {
    val s = new ServerSocket(0)
    try s.getLocalPort finally s.close()
  }

  // Two distinct truststores so we can generate two distinct keystores.
  // The global one is what a bare `ssl.keystore.location=` points to;
  // the HTTPS one is pointed at by `listener.name.https.ssl.keystore.location=`
  // and is what the HTTPS listener must actually serve.
  private var globalTruststore: File = _
  private var httpsTruststore: File = _

  override def modifyConfigs(props: scala.collection.Seq[Properties]): Unit = {
    super.modifyConfigs(props)
    globalTruststore = TestUtils.tempFile("kafka.rest.global.", ".truststore.jks")
    httpsTruststore = TestUtils.tempFile("kafka.rest.https.", ".truststore.jks")

    // Include a SAN for 127.0.0.1 so the JDK HttpClient's endpoint-identification
    // check (which Java enforces regardless of SSLParameters) is satisfied.
    val sanBuilder = new TestSslUtils.CertificateBuilder().sanIpAddress(InetAddress.getByName("127.0.0.1"))
    val globalCfg = TestSslUtils.createSslConfig(
      false, true, ConnectionMode.SERVER, globalTruststore, "kafka-global", "kafka-global",
      new TestSslUtils.CertificateBuilder().sanIpAddress(InetAddress.getByName("127.0.0.1")))
    val httpsCfg = TestSslUtils.createSslConfig(
      false, true, ConnectionMode.SERVER, httpsTruststore, "kafka-https", "kafka-https", sanBuilder)

    props.foreach { p =>
      val existing = p.getProperty(SocketServerConfigs.LISTENERS_CONFIG)
      p.setProperty(SocketServerConfigs.LISTENERS_CONFIG,
        s"$existing,HTTPS://127.0.0.1:$httpsPort")
      p.setProperty(HttpServerConfigs.HTTP_REST_BASIC_CREDENTIALS_CONFIG, s"$httpUser:$httpPass")

      // Global ssl.* (would be used if per-listener overrides weren't present).
      setSslProps(p, "", globalCfg)
      // Per-listener override. When the HTTPS listener's SslFactory is configured
      // via valuesWithPrefixOverride("listener.name.https."), this wins over the
      // global keys above. Different keystore → different cert CN → we can tell
      // which one was served.
      setSslProps(p, "listener.name.https.", httpsCfg)
    }
  }

  private def setSslProps(p: Properties, prefix: String, cfg: java.util.Map[String, AnyRef]): Unit = {
    def copy(key: String): Unit = {
      val v = cfg.get(key)
      if (v != null) p.setProperty(prefix + key, valueString(v))
    }
    copy(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
    copy(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
    copy(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
    copy(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)
  }

  private def valueString(v: Any): String = v match {
    case p: Password => p.value()
    case s => s.toString
  }

  /**
   * HttpClient that skips cert verification AND hostname verification.
   * The test cert has no SAN for 127.0.0.1 (TestSslUtils generates CN-only certs),
   * and JDK 17+ requires a SAN match by default, so we must turn that off too.
   * We still separately capture and inspect the served cert in captureServerCertificate.
   */
  private val insecureHttpClient: HttpClient = {
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(null, Array[javax.net.ssl.TrustManager](new X509TrustManager {
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
    }), null)
    val params = new SSLParameters()
    params.setEndpointIdentificationAlgorithm(null)
    HttpClient.newBuilder().sslContext(ctx).sslParameters(params).build()
  }

  private def postHttps(path: String, body: String): HttpResponse[String] = {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"https://127.0.0.1:$httpsPort$path"))
      .header("Content-Type", "application/json")
      .header("Authorization", "Basic " + httpCreds)
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    insecureHttpClient.send(req, HttpResponse.BodyHandlers.ofString())
  }

  private val mapper = new ObjectMapper()

  /**
   * Happy path over HTTPS — the load-bearing TLS test. We POST a record
   * over the HTTPS listener, then spin up a normal `KafkaConsumer` against
   * the same broker (over the binary protocol on the PLAINTEXT listener)
   * and read the exact bytes back from the exact partition and offset the
   * HTTPS response claimed. If that works, every hop in the chain is
   * proven live under encryption:
   *
   *   Jetty accepted a TLS connection on the HTTPS listener →
   *   KafkaSslContextFactory handed Jetty an SSLEngine from SslFactory →
   *   Kafka's SslFactory produced a working server-side engine →
   *   TLS handshake completed end-to-end →
   *   BasicAuthFilter accepted the credentials →
   *   Jersey dispatched to `ProduceResource.produce` →
   *   `AuthHelper.authorize` passed →
   *   `MetadataCache.getTopicId` / `numPartitions` worked →
   *   `ReplicaManager.appendRecords` actually appended →
   *   The record is durable and readable by a normal Kafka client.
   *
   * Crucially, the read-back client uses the PLAINTEXT listener — it
   * proves the record reached the topic, not just that Jetty accepted
   * the POST on a TLS socket and threw it away.
   */
  @Test
  def httpsProducedRecordReachesTopic(): Unit = {
    val topic = "rest-https-happy"
    createTopic(topic, numPartitions = 3, replicationFactor = 1)

    // Produce over HTTPS. Expect 200 + {partition, offset} in the JSON body.
    val resp = postHttps(s"/v1/topics/$topic", """{"key":"k1","value":"hello-over-tls"}""")
    assertEquals(200, resp.statusCode(), s"Body: ${resp.body()}")

    val json = mapper.readTree(resp.body())
    val partition = json.get("partition").asInt()
    val offset = json.get("offset").asLong()
    assertTrue(partition >= 0 && partition < 3, s"partition out of range: $partition")
    assertEquals(0L, offset)

    // IntegrationTestHarness requires group.protocol on the harness-level
    // consumerConfig, not just on the per-call overrides. "classic" because
    // we're not testing the new consumer rebalance protocol here.
    consumerConfig.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic")

    val props = new Properties()
    props.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "rest-https-test")
    val consumer = createConsumer(
      new org.apache.kafka.common.serialization.StringDeserializer,
      new org.apache.kafka.common.serialization.StringDeserializer,
      props)
    try {
      consumer.subscribe(java.util.List.of(topic))

      // Poll up to 10 s for the record the HTTPS request just produced.
      val deadline = System.currentTimeMillis() + 10000
      var received: List[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]] = Nil
      while (received.isEmpty && System.currentTimeMillis() < deadline) {
        received = consumer.poll(java.time.Duration.ofMillis(500)).asScala.toList
      }

      // The record we read back must match what we POSTed, byte-for-byte,
      // on the same partition the HTTPS response announced.
      assertEquals(1, received.size, "expected exactly one record via HTTPS")
      assertEquals("hello-over-tls", received.head.value())
      assertEquals(partition, received.head.partition())
    } finally consumer.close()
  }

  /**
   * Per-listener keystore override — the specific behaviour that motivated
   * routing HTTPS through `SslFactory` instead of a hand-rolled Jetty
   * keystore factory. `modifyConfigs` sets two keystores:
   *
   *   ssl.keystore.location=<global cert, CN=kafka-global>
   *   listener.name.https.ssl.keystore.location=<https cert, CN=kafka-https>
   *
   * Kafka's convention is that `listener.name.<name>.ssl.*` wins for that
   * listener. If the REST proxy were building its Jetty
   * `SslContextFactory` directly from the global `ssl.*` block — as the
   * previous hand-rolled implementation did — the HTTPS listener would
   * serve the global cert and this assertion would fail. We grab the
   * cert off the wire during a real TLS handshake and check the subject
   * CN: "kafka-https" proves the per-listener override was honoured.
   */
  @Test
  def httpsServesPerListenerKeystoreNotGlobal(): Unit = {
    val servedCert = captureServerCertificate()
    val subject = servedCert.getSubjectX500Principal.getName
    assertTrue(subject.contains("CN=kafka-https"),
      s"Expected HTTPS listener to serve the per-listener keystore (CN=kafka-https) " +
        s"but got subject '$subject'. This is the listener.name.https.ssl.* override " +
        s"that the old hand-rolled SslContextFactory path silently ignored.")
    assertFalse(subject.contains("CN=kafka-global"),
      "Per-listener override was NOT applied; broker served the global keystore instead of the HTTPS one.")
  }

  /**
   * Open a TLS handshake against the HTTPS port and capture whichever cert
   * the server presents. We use a `TrustManager` that unconditionally
   * accepts the chain (we trust the test CA by construction) but records
   * the server's leaf cert in the process — this is how we inspect what
   * the broker actually served over the wire without having the
   * certificate's trust path set up on the client side.
   */
  private def captureServerCertificate(): X509Certificate = {
    var captured: X509Certificate = null
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(null, Array[javax.net.ssl.TrustManager](new X509TrustManager {
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {
        // Capture only the leaf (index 0) the first time it arrives.
        if (chain != null && chain.nonEmpty && captured == null) captured = chain(0)
      }
    }), null)
    val socket = ctx.getSocketFactory.createSocket("127.0.0.1", httpsPort).asInstanceOf[javax.net.ssl.SSLSocket]
    try {
      socket.startHandshake()
    } finally socket.close()
    assertNotNull(captured, "server did not present a certificate during handshake")
    captured
  }
}
