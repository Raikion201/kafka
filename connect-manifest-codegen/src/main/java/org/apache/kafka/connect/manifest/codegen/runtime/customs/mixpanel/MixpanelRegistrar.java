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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.mixpanel;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry;

/**
 * Registers all {@code source_mixpanel.components.*} custom component implementations.
 *
 * <p>All implementations are stubs that return empty results; the Mixpanel connector will
 * start and remain RUNNING but produce no records until full HTTP implementations are added.
 * Full implementations require the codegen to forward the rendered URL base and stream path
 * to the component factory constructor.</p>
 */
public final class MixpanelRegistrar {

    private static final String MP = "source_mixpanel.components.";

    static {
        register();
    }

    private MixpanelRegistrar() {
    }

    /** Idempotent registration. */
    public static void register() {
        CustomComponentRegistry.register(MP + "MixpanelHttpRequester",
            MixpanelHttpRequester::new);

        CustomComponentRegistry.register(MP + "AnnotationsHttpRequester",
            AnnotationsHttpRequester::new);

        CustomComponentRegistry.register(MP + "FunnelsHttpRequester",
            FunnelsHttpRequester::new);

        CustomComponentRegistry.register(MP + "EngagesHttpRequester",
            EngagesHttpRequester::new);

        CustomComponentRegistry.register(MP + "ExportHttpRequester",
            ExportHttpRequester::new);

        CustomComponentRegistry.register(MP + "EngagePropertiesDpathExtractor",
            EngagePropertiesDpathExtractor::new);

        CustomComponentRegistry.register(MP + "ExportDpathExtractor",
            ExportDpathExtractor::new);

        CustomComponentRegistry.register(MP + "FunnelsDpathExtractor",
            FunnelsDpathExtractor::new);

        CustomComponentRegistry.register(MP + "RevenueDpathExtractor",
            RevenueDpathExtractor::new);

        CustomComponentRegistry.register(MP + "EngageTransformation",
            EngageTransformation::new);

        CustomComponentRegistry.register(MP + "PropertiesTransformation",
            PropertiesTransformation::new);
    }
}
