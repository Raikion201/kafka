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
package org.apache.kafka.connect.manifest.codegen.parser;

import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Parses an Airbyte-style {@code manifest.yaml} into a {@link ManifestSpec}.
 */
public class ManifestParser {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    /**
     * Parse a manifest from a file.
     *
     * @throws ManifestParseException if the file cannot be read or does not conform to the expected structure
     */
    public ManifestSpec parse(File file) throws ManifestParseException {
        try {
            return validate(YAML.readValue(file, ManifestSpec.class));
        } catch (IOException e) {
            throw new ManifestParseException("Failed to parse manifest file: " + file.getPath(), e);
        }
    }

    /**
     * Parse a manifest from an input stream (e.g. classpath resource in tests).
     *
     * @throws ManifestParseException if parsing fails
     */
    public ManifestSpec parse(InputStream in) throws ManifestParseException {
        try {
            return validate(YAML.readValue(in, ManifestSpec.class));
        } catch (IOException e) {
            throw new ManifestParseException("Failed to parse manifest from stream", e);
        }
    }

    private ManifestSpec validate(ManifestSpec spec) throws ManifestParseException {
        List<StreamSpec> resolved = spec.resolvedStreams();
        if (resolved.isEmpty()) {
            throw new ManifestParseException("Manifest must define at least one stream");
        }
        for (var stream : resolved) {
            if (stream.getName() == null || stream.getName().isBlank()) {
                throw new ManifestParseException("Every stream must have a non-blank name");
            }
            if (stream.getRetriever() == null) {
                throw new ManifestParseException("Stream '" + stream.getName() + "' is missing a retriever");
            }
            if (stream.getRetriever().getRequester() == null) {
                throw new ManifestParseException("Stream '" + stream.getName() + "' retriever is missing a requester");
            }
            String base = stream.getRetriever().getRequester().effectiveBaseUrl();
            if (base.isBlank()) {
                throw new ManifestParseException("Stream '" + stream.getName() + "' requester has no url or url_base");
            }
        }
        return spec;
    }
}
