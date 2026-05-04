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
package org.apache.kafka.connect.manifest.codegen.runtime.customs;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry mapping Airbyte manifest {@code class_name} strings to Java factories.
 *
 * <p>Generated source-task code looks up an instance via {@link #create(String, Class, Map, Map)}.
 * Hand-written impls register themselves in {@code static {}} blocks loaded eagerly at
 * connector start (typically via a {@code Registrar} class per connector package).</p>
 *
 * <p>If a {@code class_name} is referenced by a manifest but no factory is registered,
 * lookup throws {@link ConnectException} at task start with a clear message — connector
 * loads cleanly, task fails with "no Java implementation registered for X".</p>
 */
public final class CustomComponentRegistry {

    private static final Map<String, CustomComponentFactory<?>> FACTORIES = new ConcurrentHashMap<>();

    private CustomComponentRegistry() {
    }

    /** Register a factory for {@code className}. Last registration wins. */
    public static <T extends CustomComponent> void register(
            String className, CustomComponentFactory<T> factory) {
        if (className == null || className.isEmpty()) {
            throw new IllegalArgumentException("className must be non-empty");
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory must be non-null");
        }
        FACTORIES.put(className, factory);
    }

    /** Returns true when a factory has been registered for the given class_name. */
    public static boolean isRegistered(String className) {
        return FACTORIES.containsKey(className);
    }

    /**
     * Build a component instance for the given class_name. Caller passes the expected
     * interface type (e.g. {@link CustomTransformation}.class) for safe cast.
     *
     * @throws ConnectException when nothing is registered or the registered factory
     *         returns the wrong type.
     */
    public static <T extends CustomComponent> T create(
            String className,
            Class<T> expectedType,
            Map<String, String> connectorConfig,
            Map<String, Object> componentParams) {
        CustomComponentFactory<?> factory = FACTORIES.get(className);
        if (factory == null) {
            throw new ConnectException(
                "No Java implementation registered for class_name '" + className
                    + "'. Add a registration to the connector's customs package.");
        }
        Object instance = factory.create(
            connectorConfig == null ? Collections.emptyMap() : connectorConfig,
            componentParams == null ? Collections.emptyMap() : componentParams);
        if (instance == null) {
            throw new ConnectException("Factory for '" + className + "' returned null");
        }
        if (!expectedType.isInstance(instance)) {
            throw new ConnectException("Factory for '" + className + "' returned "
                + instance.getClass().getName() + ", expected " + expectedType.getName());
        }
        return expectedType.cast(instance);
    }

    /** Visible for tests. */
    static void clear() {
        FACTORIES.clear();
    }
}
