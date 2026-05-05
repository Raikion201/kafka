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
import java.util.List;
import java.util.Map;

/**
 * Registry of bare-name callable functions invoked from Jinja expressions like
 * {@code now_utc()}, {@code max(...)}, {@code duration('P1D')}.
 *
 * <p>The default registry is empty; concrete builtins are wired by
 * {@link Builtins#install(Functions, Filters, Tests, Methods)}.
 */
public final class Functions {

    @FunctionalInterface
    public interface Fn {
        Object apply(List<Object> args, Map<String, Object> kwargs);
    }

    private final Map<String, Fn> table = new HashMap<>();

    public void register(String name, Fn fn) {
        table.put(name, fn);
    }

    public boolean has(String name) {
        return table.containsKey(name);
    }

    public Object invoke(String name, List<Object> args, Map<String, Object> kwargs) {
        Fn fn = table.get(name);
        if (fn == null) {
            throw new JinjaException("unknown function: " + name);
        }
        return fn.apply(args, kwargs);
    }
}
