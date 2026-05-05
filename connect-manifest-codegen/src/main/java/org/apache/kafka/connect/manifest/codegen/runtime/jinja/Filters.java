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
 * Registry of pipe filters invoked as {@code value | name(args)}. The first
 * argument to each implementation is the piped target; positional args follow.
 */
public final class Filters {

    @FunctionalInterface
    public interface Filter {
        Object apply(Object target, List<Object> args);
    }

    private final Map<String, Filter> table = new HashMap<>();

    public void register(String name, Filter filter) {
        table.put(name, filter);
    }

    public Object invoke(String name, Object target, List<Object> args) {
        Filter f = table.get(name);
        if (f == null) {
            throw new JinjaException("unknown filter: " + name);
        }
        return f.apply(target, args);
    }
}
