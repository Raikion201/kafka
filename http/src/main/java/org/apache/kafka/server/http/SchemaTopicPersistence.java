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
package org.apache.kafka.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Persists schema registry state to a compacted Kafka topic ({@code _schemas}).
 *
 * <h3>Write path (async)</h3>
 * Every {@link SchemaStore#register} call fires a fire-and-forget
 * {@link KafkaProducer#send} to {@code _schemas}. The HTTP response is not
 * blocked on the produce ACK — the ID is already committed to in-memory state,
 * so the caller always gets a response. If the broker crashes before the write
 * lands, the schema ID will be re-assigned on the next restart (unlikely in
 * practice because the topic is single-partition and local).
 *
 * <h3>Read path (restore on startup)</h3>
 * {@link #restore(SchemaStore)} is called once during {@code HttpRestServer.startup()}.
 * It reads all messages from {@code _schemas} partition 0 from the earliest offset
 * and replays them into an empty {@link SchemaStore}, rebuilding the full in-memory
 * state before the HTTP server begins accepting requests.
 *
 * <h3>Message format</h3>
 * Key: {@code subject} string (enables per-subject compaction).
 * Value: JSON — either a registration event or a deletion tombstone:
 * <pre>
 *   Registration: {"id":1,"subject":"user-value","version":1,"schema":"..."}
 *   Deletion:     {"subject":"user-value","deleted":true}
 * </pre>
 */
public final class SchemaTopicPersistence implements AutoCloseable {

    static final String SCHEMAS_TOPIC = "_schemas";

    private static final Logger LOG = LoggerFactory.getLogger(SchemaTopicPersistence.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final KafkaProducer<String, String> producer;
    private final String bootstrapServers;

    public SchemaTopicPersistence(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "schema-registry-internal");
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Async fire-and-forget write of a schema registration event to {@code _schemas}.
     * Failures are logged at WARN but never propagate — in-memory state is already committed.
     */
    public void persistRegistration(String subject, int id, int version, String schema) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("id", id);
            node.put("subject", subject);
            node.put("version", version);
            node.put("schema", schema);
            ProducerRecord<String, String> record =
                new ProducerRecord<>(SCHEMAS_TOPIC, subject, node.toString());
            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    LOG.warn("Failed to persist schema registration id={} subject={} to {}: {}",
                        id, subject, SCHEMAS_TOPIC, ex.getMessage());
                }
            });
        } catch (Exception e) {
            LOG.warn("Failed to send schema registration to {}: {}", SCHEMAS_TOPIC, e.getMessage());
        }
    }

    /**
     * Async fire-and-forget write of a deletion tombstone (null value) to {@code _schemas}.
     */
    public void persistDeletion(String subject) {
        try {
            // null value = tombstone — compaction will eventually remove the subject's records
            ProducerRecord<String, String> record =
                new ProducerRecord<>(SCHEMAS_TOPIC, subject, null);
            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    LOG.warn("Failed to persist deletion tombstone subject={} to {}: {}",
                        subject, SCHEMAS_TOPIC, ex.getMessage());
                }
            });
        } catch (Exception e) {
            LOG.warn("Failed to send deletion tombstone to {}: {}", SCHEMAS_TOPIC, e.getMessage());
        }
    }

    /**
     * Replay all messages in {@code _schemas} from the beginning into {@code store}.
     * Called once at broker startup before the HTTP server accepts requests.
     * Silently skips if the topic does not yet exist.
     */
    public void restore(SchemaStore store) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-restore-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(SCHEMAS_TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));
            consumer.seekToBeginning(Collections.singletonList(partition));

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(partition));
            long endOffset = endOffsets.getOrDefault(partition, 0L);

            if (endOffset == 0) {
                LOG.info("_schemas topic is empty — starting with clean schema registry state");
                return;
            }

            int restored = 0;
            while (consumer.position(partition) < endOffset) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) {
                        // tombstone — subject was deleted
                        store.deleteSubject(record.key());
                        continue;
                    }
                    try {
                        var node = MAPPER.readTree(record.value());
                        if (node.has("deleted")) {
                            store.deleteSubject(record.key());
                        } else {
                            int id = node.get("id").asInt();
                            String subject = node.get("subject").asText();
                            String schema = node.get("schema").asText();
                            store.restoreRegistration(subject, id, schema);
                            restored++;
                        }
                    } catch (Exception e) {
                        LOG.warn("Skipping malformed _schemas record at offset {}: {}",
                            record.offset(), e.getMessage());
                    }
                }
            }
            LOG.info("Restored {} schema(s) from {} topic", restored, SCHEMAS_TOPIC);
        } catch (Exception e) {
            LOG.warn("Could not restore from {} (topic may not exist yet): {}", SCHEMAS_TOPIC, e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            producer.flush();
            producer.close();
        } catch (Exception e) {
            LOG.warn("Error closing schema registry producer: {}", e.getMessage());
        }
    }
}
