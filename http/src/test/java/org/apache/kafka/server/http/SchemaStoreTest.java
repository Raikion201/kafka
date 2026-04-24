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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaStoreTest {

    private SchemaStore store;

    @BeforeEach
    void setUp() {
        store = new SchemaStore();
    }

    @Test
    void registerNewSchema_assignsIdStartingAtOne() {
        int id = store.register("user-value", "{\"type\":\"string\"}");
        assertEquals(1, id);
    }

    @Test
    void registerSameSchemaAgain_returnsSameId() {
        int id1 = store.register("user-value", "{\"type\":\"string\"}");
        int id2 = store.register("user-value", "{\"type\":\"string\"}");
        assertEquals(id1, id2);
    }

    @Test
    void registerSameSchemaUnderDifferentSubject_returnsSameId() {
        int id1 = store.register("user-value", "{\"type\":\"string\"}");
        int id2 = store.register("order-value", "{\"type\":\"string\"}");
        assertEquals(id1, id2, "same content must get same global id regardless of subject");
    }

    @Test
    void registerDifferentSchema_assignsNewId() {
        int id1 = store.register("user-value", "{\"type\":\"string\"}");
        int id2 = store.register("user-value", "{\"type\":\"int\"}");
        assertNotEquals(id1, id2);
        assertEquals(2, id2);
    }

    @Test
    void getById_returnsSchema() {
        int id = store.register("user-value", "{\"type\":\"string\"}");
        assertEquals("{\"type\":\"string\"}", store.getById(id));
    }

    @Test
    void getById_unknownId_returnsNull() {
        assertNull(store.getById(999));
    }

    @Test
    void getVersions_returnsListInRegistrationOrder() {
        store.register("user-value", "{\"type\":\"string\"}");
        store.register("user-value", "{\"type\":\"int\"}");
        List<Integer> versions = store.getVersions("user-value");
        assertEquals(2, versions.size());
        assertEquals(1, versions.get(0));
        assertEquals(2, versions.get(1));
    }

    @Test
    void getVersions_unknownSubject_returnsEmptyList() {
        assertTrue(store.getVersions("no-such-subject").isEmpty());
    }

    @Test
    void getVersion_returnsCorrectId_oneBased() {
        int id1 = store.register("user-value", "{\"type\":\"string\"}");
        int id2 = store.register("user-value", "{\"type\":\"int\"}");

        assertEquals(id1, store.getVersion("user-value", 1));
        assertEquals(id2, store.getVersion("user-value", 2));
    }

    @Test
    void getVersion_zeroOrNegative_returnsNull() {
        store.register("user-value", "{\"type\":\"string\"}");
        assertNull(store.getVersion("user-value", 0));
        assertNull(store.getVersion("user-value", -1));
    }

    @Test
    void getVersion_beyondSize_returnsNull() {
        store.register("user-value", "{\"type\":\"string\"}");
        assertNull(store.getVersion("user-value", 2));
    }

    @Test
    void getVersion_unknownSubject_returnsNull() {
        assertNull(store.getVersion("no-such-subject", 1));
    }

    @Test
    void subjectOf_returnsFirstSubjectAndVersion() {
        int id = store.register("user-value", "{\"type\":\"string\"}");
        SchemaStore.SubjectVersion sv = store.subjectOf(id);
        assertNotNull(sv);
        assertEquals("user-value", sv.subject());
        assertEquals(1, sv.version());
    }

    @Test
    void subjectOf_unknownId_returnsNull() {
        assertNull(store.subjectOf(999));
    }

    @Test
    void deleteSubject_removesSubjectAndReturnsVersionIds() {
        int id1 = store.register("user-value", "{\"type\":\"string\"}");
        int id2 = store.register("user-value", "{\"type\":\"int\"}");

        List<Integer> deleted = store.deleteSubject("user-value");
        assertEquals(List.of(id1, id2), deleted);
        assertTrue(store.getVersions("user-value").isEmpty());
    }

    @Test
    void deleteSubject_doesNotRemoveSchemaContentFromStore() {
        int id = store.register("user-value", "{\"type\":\"string\"}");
        store.deleteSubject("user-value");
        // schema content stays — global id still resolvable
        assertEquals("{\"type\":\"string\"}", store.getById(id));
    }

    @Test
    void deleteSubject_unknownSubject_returnsEmptyList() {
        assertTrue(store.deleteSubject("no-such-subject").isEmpty());
    }

    @Test
    void concurrentRegistrations_noIdCollisions() throws Exception {
        int threads = 20;
        int schemasPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Set<Integer> assignedIds = ConcurrentHashMap.newKeySet();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> {
                for (int s = 0; s < schemasPerThread; s++) {
                    String schema = "{\"thread\":" + threadId + ",\"seq\":" + s + "}";
                    int id = store.register("subject-" + threadId, schema);
                    assignedIds.add(id);
                }
            }));
        }

        for (Future<?> f : futures) f.get();
        pool.shutdown();

        // every unique schema should have a unique id
        int uniqueSchemas = threads * schemasPerThread;
        assertEquals(uniqueSchemas, assignedIds.size(),
                "concurrent registrations produced duplicate ids");
    }
}
