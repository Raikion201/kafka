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

import java.util.Locale;

/**
 * Schema compatibility modes, same semantics as Confluent Schema Registry.
 *
 * <ul>
 *   <li>{@link #BACKWARD} — new schema can read data written with the old schema.
 *       Deploy consumers first, then producers.</li>
 *   <li>{@link #FORWARD} — old schema can read data written with the new schema.
 *       Deploy producers first, then consumers.</li>
 *   <li>{@link #FULL} — both backward and forward. Safest, most restrictive.</li>
 *   <li>{@link #NONE} — no compatibility check. Anything goes.</li>
 * </ul>
 */
public enum SchemaCompatibility {
    BACKWARD,
    FORWARD,
    FULL,
    NONE;

    public static SchemaCompatibility fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Unknown compatibility mode: '" + value + "'. Must be one of: BACKWARD, FORWARD, FULL, NONE");
        }
    }
}
