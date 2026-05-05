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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomerClientFilterTest {

    private Map<String, Object> rec(Object id, String status, String clientCustomer) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", id);
        r.put("status", status);
        r.put("clientCustomer", clientCustomer);
        return r;
    }

    @Test
    void dropsRecordsNotInCustomerIdsAllowlist() {
        CustomerClientFilter f = new CustomerClientFilter(
            Collections.emptyMap(),
            Map.of("customer_ids", List.of("111")));

        assertTrue(f.accept(rec("111", "ENABLED", "customers/A")));
        assertFalse(f.accept(rec("222", "ENABLED", "customers/B")));
    }

    @Test
    void dropsRecordsNotInStatusAllowlist() {
        CustomerClientFilter f = new CustomerClientFilter(
            Collections.emptyMap(),
            Map.of("customer_status_filter", List.of("ENABLED")));

        assertTrue(f.accept(rec("1", "ENABLED", "x")));
        assertFalse(f.accept(rec("2", "PAUSED", "y")));
    }

    @Test
    void defaultStatusListAcceptsKnownStates() {
        CustomerClientFilter f = new CustomerClientFilter(Collections.emptyMap(), Collections.emptyMap());
        assertTrue(f.accept(rec("1", "ENABLED", "a")));
        assertTrue(f.accept(rec("2", "CLOSED", "b")));
        assertFalse(f.accept(rec("3", "WEIRD", "c")));
    }

    @Test
    void dedupesByClientCustomer() {
        CustomerClientFilter f = new CustomerClientFilter(Collections.emptyMap(), Collections.emptyMap());
        assertTrue(f.accept(rec("1", "ENABLED", "customers/A")));
        assertFalse(f.accept(rec("1", "ENABLED", "customers/A")));
        assertTrue(f.accept(rec("2", "ENABLED", "customers/B")));
    }

    @Test
    void rejectsNullRecord() {
        CustomerClientFilter f = new CustomerClientFilter(Collections.emptyMap(), Collections.emptyMap());
        assertFalse(f.accept(null));
    }

    @Test
    void rejectsRecordMissingUniqueKey() {
        CustomerClientFilter f = new CustomerClientFilter(Collections.emptyMap(), Collections.emptyMap());
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", "1");
        r.put("status", "ENABLED");
        // no clientCustomer
        assertFalse(f.accept(r));
    }

    @Test
    void readsCustomerIdsFromConnectorConfigCsv() {
        CustomerClientFilter f = new CustomerClientFilter(
            Map.of("customer_ids", "100,200"),
            Collections.emptyMap());

        assertTrue(f.accept(rec("100", "ENABLED", "a")));
        assertFalse(f.accept(rec("999", "ENABLED", "b")));
    }
}
