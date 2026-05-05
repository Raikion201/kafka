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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoogleAdsCriterionParentStateMigrationTest {

    private final GoogleAdsCriterionParentStateMigration m =
        new GoogleAdsCriterionParentStateMigration(Collections.emptyMap(), Collections.emptyMap());

    @Test
    void migratesLegacyShape() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("cursor", "x");
        assertTrue(m.shouldMigrate(state));

        Map<String, Object> out = m.migrate(state);
        @SuppressWarnings("unchecked")
        Map<String, Object> parent = (Map<String, Object>) out.get("parent_state");
        // Whole legacy state is nested under change_status (matches Python).
        assertEquals(state, parent.get("change_status"));
    }

    @Test
    void passesThroughAlreadyMigratedShape() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("parent_state", Map.of("change_status", Map.of()));
        assertFalse(m.shouldMigrate(state));
        assertSame(state, m.migrate(state));
    }

    @Test
    void emptyAndNullStateNotMigrated() {
        assertFalse(m.shouldMigrate(null));
        assertFalse(m.shouldMigrate(Collections.emptyMap()));
    }
}
