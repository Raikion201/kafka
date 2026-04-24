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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe in-memory schema registry store.
 *
 * <p>The store is intentionally simple — your boss's "glorified hashmap".
 * Three maps give O(1) access for every operation:</p>
 * <ul>
 *   <li>{@code schemaToId} — deduplication: same content → same ID</li>
 *   <li>{@code idToSchema} — fetch schema by ID</li>
 *   <li>{@code subjectVersions} — ordered list of IDs per subject (versions)</li>
 * </ul>
 *
 * <p>All state is in-memory. Schemas are lost on broker restart.</p>
 */
public final class SchemaStore {

    // schema content → global id (deduplication across all subjects)
    private final ConcurrentHashMap<String, Integer> schemaToId = new ConcurrentHashMap<>();

    // global id → schema content
    private final ConcurrentHashMap<Integer, String> idToSchema = new ConcurrentHashMap<>();

    // subject → ordered list of schema IDs (index 0 = version 1)
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> subjectVersions =
            new ConcurrentHashMap<>();

    // reverse: id → first (subject, 1-based version) that registered it
    private final ConcurrentHashMap<Integer, SubjectVersion> idToSubjectVersion =
            new ConcurrentHashMap<>();

    private final AtomicInteger nextId = new AtomicInteger(1);

    /**
     * Register a schema under a subject. If the exact same schema content has
     * already been registered (under any subject), the existing ID is returned
     * without creating a new version. Otherwise a new ID is assigned and a new
     * version is appended to the subject's version list.
     *
     * @param subject the subject name (e.g. {@code "user-value"})
     * @param schema  the schema content string
     * @return the schema ID (existing or newly assigned)
     */
    public int register(String subject, String schema) {
        // computeIfAbsent is atomic — only one thread assigns a new id for a given schema
        int id = schemaToId.computeIfAbsent(schema, k -> {
            int newId = nextId.getAndIncrement();
            idToSchema.put(newId, schema);
            return newId;
        });

        // Append id to subject's version list if not already present for this subject.
        // addIfAbsent() is atomic on CopyOnWriteArrayList — no external lock needed.
        CopyOnWriteArrayList<Integer> versions =
                subjectVersions.computeIfAbsent(subject, k -> new CopyOnWriteArrayList<>());

        boolean added = versions.addIfAbsent(id);
        if (added) {
            // indexOf returns the index in the snapshot that includes our just-added
            // element. +1 converts to 1-based version number.
            int version = versions.indexOf(id) + 1;
            idToSubjectVersion.putIfAbsent(id, new SubjectVersion(subject, version));
        }

        return id;
    }

    /**
     * Fetch a schema by its global ID.
     *
     * @param id the schema ID
     * @return the schema content, or {@code null} if not found
     */
    public String getById(int id) {
        return idToSchema.get(id);
    }

    /**
     * List all schema IDs registered under a subject, ordered by version.
     *
     * @param subject the subject name
     * @return immutable list of IDs (version 1 first); empty if subject not found
     */
    public List<Integer> getVersions(String subject) {
        CopyOnWriteArrayList<Integer> versions = subjectVersions.get(subject);
        if (versions == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(versions));
    }

    /**
     * Fetch the schema ID for a specific version of a subject.
     *
     * @param subject the subject name
     * @param version 1-based version number
     * @return the schema ID, or {@code null} if subject or version not found
     */
    public Integer getVersion(String subject, int version) {
        CopyOnWriteArrayList<Integer> versions = subjectVersions.get(subject);
        if (versions == null || version < 1 || version > versions.size()) {
            return null;
        }
        return versions.get(version - 1);
    }

    /**
     * Look up the first subject and version that registered a given schema ID.
     * Used to populate {@link SchemaVersionBody} when fetching by ID.
     *
     * @param id the schema ID
     * @return the subject+version pair, or {@code null} if the ID is unknown
     */
    public SubjectVersion subjectOf(int id) {
        return idToSubjectVersion.get(id);
    }

    /**
     * Delete a subject and all its versions.
     * The schema content and global IDs remain in the store — only the
     * subject mapping is removed.
     *
     * @param subject the subject name
     * @return the list of IDs that were registered under this subject
     *         (ordered by version); empty if subject was not found
     */
    public List<Integer> deleteSubject(String subject) {
        CopyOnWriteArrayList<Integer> versions = subjectVersions.remove(subject);
        if (versions == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(versions));
    }

    /** Immutable pair of subject name and 1-based version number. */
    public record SubjectVersion(String subject, int version) { }

    /** Returns a snapshot of all subjects and their version counts — for diagnostics. */
    public Map<String, Integer> subjectSummary() {
        ConcurrentHashMap<String, Integer> summary = new ConcurrentHashMap<>();
        subjectVersions.forEach((subject, versions) -> summary.put(subject, versions.size()));
        return Collections.unmodifiableMap(summary);
    }
}
