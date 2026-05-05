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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.googleads;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoogleAdsPerPartitionStateMigrationTest {

    private final GoogleAdsPerPartitionStateMigration m =
        new GoogleAdsPerPartitionStateMigration(Collections.emptyMap(), Collections.emptyMap());

    @Test
    void shouldMigrateLegacyShape() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("1234", Map.of("segments.date", "2024-01-01"));
        assertTrue(m.shouldMigrate(state));
    }

    @Test
    void doesNotMigrateAlreadyMigratedShape() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("state", Map.of("segments.date", "2024-01-01"));
        assertFalse(m.shouldMigrate(state));
    }

    @Test
    void emptyAndNullStateNotMigrated() {
        assertFalse(m.shouldMigrate(null));
        assertFalse(m.shouldMigrate(Collections.emptyMap()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void migrateBuildsStatesListAndMinState() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("111", Map.of("segments.date", "2024-01-05"));
        state.put("222", Map.of("segments.date", "2024-01-01"));

        Map<String, Object> out = m.migrate(state);

        assertEquals(Map.of("segments.date", "2024-01-01"), out.get("state"));
        List<Map<String, Object>> states = (List<Map<String, Object>>) out.get("states");
        assertEquals(2, states.size());
        Map<String, Object> p0 = (Map<String, Object>) states.get(0).get("partition");
        assertEquals("111", p0.get("customer_id"));
        assertTrue(p0.containsKey("parent_slice"));
    }

    @Test
    void migratePassesThroughWhenAlreadyMigrated() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("state", Map.of("segments.date", "2024-01-01"));
        assertSame(state, m.migrate(state));
    }

    @Test
    void migrateReturnsEmptyWhenNoCursorFields() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("1234", Map.of("not_cursor", "x"));
        assertEquals(Collections.emptyMap(), m.migrate(state));
    }

    @Test
    void honoursCustomCursorField() {
        GoogleAdsPerPartitionStateMigration custom = new GoogleAdsPerPartitionStateMigration(
            Collections.emptyMap(), Map.of("cursor_field", "change.last_change_date_time"));

        Map<String, Object> state = new LinkedHashMap<>();
        state.put("111", Map.of("change.last_change_date_time", "2024-01-01"));
        Map<String, Object> out = custom.migrate(state);
        assertTrue(out.containsKey("states"));
    }
}
