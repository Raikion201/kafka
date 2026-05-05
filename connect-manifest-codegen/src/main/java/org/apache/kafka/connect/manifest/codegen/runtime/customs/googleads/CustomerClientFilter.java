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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordFilter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Java port of {@code source_google_ads.components.CustomerClientFilter}
 * (components.py:118-147).
 *
 * <p>Three filter passes per record, in order:</p>
 * <ol>
 *     <li>Drop records whose {@code id} is not in the configured {@code customer_ids}
 *         allowlist (when an allowlist is provided).</li>
 *     <li>Drop records whose {@code status} is not in the configured
 *         {@code customer_status_filter} list, defaulting to
 *         {@code [UNKNOWN, ENABLED, CANCELED, SUSPENDED, CLOSED]} when unset.</li>
 *     <li>Deduplicate by the unique key {@code clientCustomer} — keep first occurrence.</li>
 * </ol>
 *
 * <p>The original Python filter is stateful (a per-instance {@code _seen_keys} set);
 * this Java port keeps the same per-instance contract via {@link #accept}. Stateful
 * filters are fine here because the runtime instantiates one filter per stream task.</p>
 */
public final class CustomerClientFilter implements CustomRecordFilter {

    private static final String UNIQUE_KEY = "clientCustomer";
    private static final List<String> DEFAULT_STATUSES =
        Arrays.asList("UNKNOWN", "ENABLED", "CANCELED", "SUSPENDED", "CLOSED");

    private final Set<String> allowedCustomerIds;
    private final Set<String> allowedStatuses;
    private final Set<Object> seenKeys = new HashSet<>();

    public CustomerClientFilter(Map<String, String> connectorConfig,
                                Map<String, Object> componentParams) {
        Object customerIds = componentParams == null ? null : componentParams.get("customer_ids");
        if (customerIds == null && connectorConfig != null) {
            // Python reads from `self.config["customer_ids"]`; in Java we accept either source.
            String s = connectorConfig.get("customer_ids");
            if (s != null && !s.isEmpty()) {
                customerIds = Arrays.asList(s.split(","));
            }
        }
        this.allowedCustomerIds = toStringSet(customerIds);

        Object statuses = componentParams == null ? null : componentParams.get("customer_status_filter");
        if (statuses == null && connectorConfig != null) {
            String s = connectorConfig.get("customer_status_filter");
            if (s != null && !s.isEmpty()) {
                statuses = Arrays.asList(s.split(","));
            }
        }
        this.allowedStatuses = statuses != null ? toStringSet(statuses) : new HashSet<>(DEFAULT_STATUSES);
    }

    @Override
    public boolean accept(Map<String, Object> record) {
        if (record == null) {
            return false;
        }

        if (allowedCustomerIds != null) {
            Object id = record.get("id");
            if (id == null || !allowedCustomerIds.contains(id.toString())) {
                return false;
            }
        }

        Object status = record.get("status");
        if (status == null || !allowedStatuses.contains(status.toString())) {
            return false;
        }

        Object key = record.get(UNIQUE_KEY);
        if (key == null) {
            return false;
        }
        return seenKeys.add(key);
    }

    private static Set<String> toStringSet(Object raw) {
        if (raw == null) {
            return null;
        }
        Set<String> set = new HashSet<>();
        if (raw instanceof Collection) {
            for (Object o : (Collection<?>) raw) {
                if (o != null) {
                    set.add(o.toString().trim());
                }
            }
        } else {
            for (String s : raw.toString().split(",")) {
                set.add(s.trim());
            }
        }
        return set;
    }
}
