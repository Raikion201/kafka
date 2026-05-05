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
package org.apache.kafka.connect.manifest.codegen.runtime.jinja;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Registry of {@code is}-tests. Each test takes a target and returns boolean.
 * Built-ins {@code none}, {@code defined}, {@code string}, {@code mapping},
 * {@code sequence}, {@code number}, {@code boolean} are pre-registered.
 */
public final class Tests {

    private final Map<String, Predicate<Object>> table = new HashMap<>();

    public Tests() {
        register("none", o -> o == null);
        register("defined", o -> o != null);
        register("undefined", o -> o == null);
        register("string", o -> o instanceof CharSequence);
        register("mapping", o -> o instanceof Map<?, ?>);
        register("sequence", o -> o instanceof Iterable<?> && !(o instanceof Map<?, ?>));
        register("number", o -> o instanceof Number);
        register("boolean", o -> o instanceof Boolean);
        register("integer", o -> o instanceof Long || o instanceof Integer);
        register("float", o -> o instanceof Double || o instanceof Float);
        register("iterable", o -> o instanceof Iterable<?> || o instanceof Map<?, ?>
            || o instanceof CharSequence || (o != null && o.getClass().isArray()));
    }

    public void register(String name, Predicate<Object> pred) {
        table.put(name, pred);
    }

    public boolean invoke(String name, Object target) {
        Predicate<Object> p = table.get(name);
        if (p == null) {
            throw new JinjaException("unknown test: " + name);
        }
        return p.test(target);
    }
}
