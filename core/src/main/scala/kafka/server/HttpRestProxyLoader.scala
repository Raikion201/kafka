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

import java.util.ServiceLoader

import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.http.api._
import org.apache.kafka.storage.internals.log.AppendOrigin

import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/**
 * Bridges the broker (Scala, core) to the embedded HTTP REST proxy (Java,
 * :http module). At broker startup, if `listeners=` contains any HTTP/HTTPS
 * entries, we:
 *
 *   1. look up a [[BrokerHttpServerFactory]] via [[ServiceLoader]] — the
 *      `:http` jar registers its factory in
 *      `META-INF/services/org.apache.kafka.server.http.api.BrokerHttpServerFactory`;
 *   2. wrap [[ReplicaManager.appendRecords]] and [[AuthHelper.authorize]] in
 *      the narrow [[RecordAppender]] and [[AuthorizationHelper]] interfaces;
 *   3. hand the resulting [[BrokerHttpServerContext]] to the factory.
 *
 * If no factory is on the runtime classpath (e.g. a stripped-down broker
 * distribution) we log a warning and return null — the broker carries on
 * without the REST proxy rather than failing to boot.
 */
object HttpRestProxyLoader {

  private val log = LoggerFactory.getLogger(getClass)

  def load(endpoints: Seq[HttpEndpoint],
           config: KafkaConfig,
           replicaManager: ReplicaManager,
           authorizerPlugin: Option[Plugin[Authorizer]],
           metadataCache: MetadataCache,
           time: Time): BrokerHttpServer = {

    val factory = ServiceLoader.load(classOf[BrokerHttpServerFactory]).iterator()
    if (!factory.hasNext) {
      log.warn("listeners= contains HTTP/HTTPS entries but no BrokerHttpServerFactory implementation " +
        "was found on the classpath. Is the :http module on the runtime classpath? Skipping REST proxy.")
      return null
    }

    val appender: RecordAppender = (timeoutMs, acks, entries, callback) =>
      replicaManager.appendRecords(
        timeout = timeoutMs,
        requiredAcks = acks,
        internalTopicsAllowed = false,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entries.asScala.toMap,
        responseCallback = result => callback.accept(result))

    val authHelper = new AuthHelper(authorizerPlugin)
    val auth: AuthorizationHelper = (ctx, op, rt, name) =>
      authHelper.authorize(ctx, op, rt, name)

    val registry: ReconfigurableRegistry = new ReconfigurableRegistry {
      override def addReconfigurable(r: org.apache.kafka.common.Reconfigurable): Unit = config.addReconfigurable(r)
      override def removeReconfigurable(r: org.apache.kafka.common.Reconfigurable): Unit = config.removeReconfigurable(r)
    }

    val context = new BrokerHttpServerContext(
      endpoints.asJava,
      config.httpExecutorThreads,
      config.httpBasicCredentials.asJava,
      config,
      registry,
      appender,
      auth,
      metadataCache,
      time)

    factory.next().create(context)
  }
}
