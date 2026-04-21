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
package org.apache.kafka.server.http.api;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Narrow view of {@code ReplicaManager.appendRecords} exposed to the
 * {@code :http} module so it can append produce requests without seeing the
 * full ReplicaManager surface. The broker supplies an adapter at startup.
 */
public interface RecordAppender {

    /**
     * Append {@code records} to the given partition. The callback fires once
     * the append has been replicated according to {@code requiredAcks}, or
     * immediately on error.
     *
     * @param timeoutMs            max time the append may block waiting for replication
     * @param requiredAcks         1 = leader only, -1 = all ISR
     * @param entries              records per partition (always one entry for the REST produce path)
     * @param responseCallback     invoked with the per-partition response map
     */
    void appendRecords(long timeoutMs,
                       short requiredAcks,
                       Map<TopicIdPartition, MemoryRecords> entries,
                       Consumer<Map<TopicIdPartition, PartitionResponse>> responseCallback);
}
