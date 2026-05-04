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

import java.util.Map;

/**
 * Java equivalent of Airbyte CDK {@code StateMigration} (and {@code CustomStateMigration}).
 * Reshapes connector state read from previous runs into the schema the current code expects.
 */
public interface CustomStateMigration extends CustomComponent {

    /** Returns true if {@code state} matches an older shape this migration can upgrade. */
    boolean shouldMigrate(Map<String, Object> state);

    /** Returns state in the new shape. Only called when {@link #shouldMigrate} returned true. */
    Map<String, Object> migrate(Map<String, Object> state);
}
