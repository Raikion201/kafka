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
package org.apache.kafka.connect.manifest.codegen.acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Drives a generated {@link SourceTask} through Airbyte-style acceptance phases:
 *
 * <ol>
 *   <li><b>read</b> — start the task with the supplied config and an empty offset reader,
 *       poll until {@code max_polls} or 3 consecutive empty polls, then assert each
 *       declared stream produced at least its {@code min_records} count.</li>
 *   <li><b>state-resume</b> — issue {@code resume_after_polls} polls, snapshot every
 *       {@code (sourcePartition → sourceOffset)} pair, stop the task, instantiate a
 *       fresh task with an offset reader returning the snapshot, poll again and
 *       assert no exception is thrown and the offset map advances or stays stable
 *       (never regresses).</li>
 * </ol>
 *
 * Skip semantics: if any value in {@link AcceptanceConfig#getConfig()} contains a
 * {@code ${ENV}} placeholder for an unset environment variable, every phase returns
 * a {@link Result} with {@code skipped=true} and a human-readable reason. Tests
 * should map this to JUnit's {@code Assumptions.assumeFalse(result.skipped, ...)}
 * so CI shows a skipped test, not a failure — same as Airbyte's CAT behaviour.
 */
public final class AcceptanceTestHarness {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final Pattern ENV_VAR = Pattern.compile("\\$\\{([A-Z0-9_]+)}");

    private AcceptanceTestHarness() {
    }

    public static AcceptanceConfig load(Path yaml) throws IOException {
        return YAML.readValue(yaml.toFile(), AcceptanceConfig.class);
    }

    /**
     * Substitutes {@code ${ENV}} placeholders against {@link System#getenv()}.
     * Returns {@code null} if any required env var is missing — callers treat this
     * as a skip, mirroring Airbyte's "no creds → skip" semantics.
     */
    public static Map<String, String> resolveConfig(Map<String, String> raw, Map<String, String> env) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            String resolved = substituteEnv(entry.getValue(), env);
            if (resolved == null) return null;
            out.put(entry.getKey(), resolved);
        }
        return out;
    }

    private static String substituteEnv(String value, Map<String, String> env) {
        if (value == null) return null;
        Matcher m = ENV_VAR.matcher(value);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            String replacement = env.get(name);
            if (replacement == null) return null;
            m.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(out);
        return out.toString();
    }

    /** Runs the full {@code read} phase. Never throws on stream-level errors; surfaces them via {@link Result}. */
    public static Result runRead(AcceptanceConfig cfg, Map<String, String> env) throws Exception {
        Map<String, String> resolved = resolveConfig(cfg.getConfig(), env);
        if (resolved == null) {
            return Result.skipped("missing env var(s) for connector " + cfg.getConnector());
        }

        SourceTask task = instantiateTask(cfg.getConnector());
        InMemoryOffsets offsets = new InMemoryOffsets();
        task.initialize(contextFor(offsets));

        Map<String, Integer> perStreamCount = new HashMap<>();
        Result result = new Result();
        try {
            task.start(resolved);
            int emptyStreak = 0;
            for (int i = 0; i < cfg.getMaxPolls() && emptyStreak < 3; i++) {
                List<SourceRecord> batch = safePoll(task, result);
                if (batch == null) return result;          // hard error captured in result
                if (batch.isEmpty()) {
                    emptyStreak++;
                    continue;
                }
                emptyStreak = 0;
                for (SourceRecord rec : batch) {
                    perStreamCount.merge(rec.topic(), 1, Integer::sum);
                    offsets.update(rec.sourcePartition(), rec.sourceOffset());
                }
            }
        } finally {
            stopQuietly(task);
        }

        for (AcceptanceConfig.StreamSpec spec : cfg.getStreams()) {
            int got = perStreamCount.getOrDefault(spec.getName(), 0);
            if (got < spec.getMinRecords()) {
                result.failures.add("stream '" + spec.getName() + "' produced " + got
                    + " records, expected >= " + spec.getMinRecords());
            }
            result.recordCounts.put(spec.getName(), got);
        }
        result.finalOffsets = offsets.snapshot();
        return result;
    }

    /** Runs the {@code state-resume} phase: poll, stop, restart with saved offsets, poll again. */
    public static Result runStateResume(AcceptanceConfig cfg, Map<String, String> env) throws Exception {
        Map<String, String> resolved = resolveConfig(cfg.getConfig(), env);
        if (resolved == null) {
            return Result.skipped("missing env var(s) for connector " + cfg.getConnector());
        }

        Result result = new Result();
        InMemoryOffsets offsets = new InMemoryOffsets();

        // Phase A: poll N times, snapshot offsets.
        if (!pollPhaseA(cfg, resolved, offsets, result)) return result;
        Map<Map<String, Object>, Map<String, Object>> snapshot = offsets.snapshot();

        // Phase B: fresh task, same offsets, must not throw and must not regress.
        InMemoryOffsets resumed = new InMemoryOffsets();
        resumed.bulkLoad(snapshot);
        if (!pollPhaseB(cfg, resolved, snapshot, resumed, result)) return result;
        result.finalOffsets = resumed.snapshot();
        return result;
    }

    private static boolean pollPhaseA(AcceptanceConfig cfg, Map<String, String> resolved,
                                      InMemoryOffsets offsets, Result result) throws Exception {
        SourceTask task = instantiateTask(cfg.getConnector());
        task.initialize(contextFor(offsets));
        try {
            task.start(resolved);
            for (int i = 0; i < cfg.getResumeAfterPolls(); i++) {
                List<SourceRecord> batch = safePoll(task, result);
                if (batch == null) return false;
                for (SourceRecord rec : batch) {
                    offsets.update(rec.sourcePartition(), rec.sourceOffset());
                    result.recordCounts.merge(rec.topic(), 1, Integer::sum);
                }
            }
            return true;
        } finally {
            stopQuietly(task);
        }
    }

    private static boolean pollPhaseB(AcceptanceConfig cfg, Map<String, String> resolved,
                                      Map<Map<String, Object>, Map<String, Object>> snapshot,
                                      InMemoryOffsets resumed, Result result) throws Exception {
        SourceTask task = instantiateTask(cfg.getConnector());
        task.initialize(contextFor(resumed));
        try {
            task.start(resolved);
            for (int i = 0; i < cfg.getResumeAfterPolls(); i++) {
                List<SourceRecord> batch = safePoll(task, result);
                if (batch == null) return false;
                applyPhaseBBatch(batch, snapshot, resumed, result);
            }
            return true;
        } finally {
            stopQuietly(task);
        }
    }

    private static void applyPhaseBBatch(List<SourceRecord> batch,
                                         Map<Map<String, Object>, Map<String, Object>> snapshot,
                                         InMemoryOffsets resumed, Result result) {
        for (SourceRecord rec : batch) {
            Map<String, Object> prev = snapshot.get(rec.sourcePartition());
            @SuppressWarnings("unchecked")
            Map<String, Object> off = (Map<String, Object>) rec.sourceOffset();
            if (prev != null && offsetRegressed(prev, off)) {
                result.failures.add("offset regressed for partition " + rec.sourcePartition()
                    + ": " + prev + " -> " + rec.sourceOffset());
            }
            resumed.update(rec.sourcePartition(), rec.sourceOffset());
            result.recordCounts.merge(rec.topic(), 1, Integer::sum);
        }
    }

    private static void stopQuietly(SourceTask task) {
        try {
            task.stop();
        } catch (Exception ignored) {
            // best effort; do not mask the primary outcome
        }
    }

    /**
     * A regression is when a numeric offset key (partition_idx, position, cursor index)
     * decreases. We compare matching keys numerically when both are numbers; otherwise
     * we treat the change as non-regressing (string cursors like RFC3339 timestamps are
     * lexicographically monotonic in well-formed cursors, so a strict-decrease is also a
     * regression — we apply the same rule there).
     */
    private static boolean offsetRegressed(Map<String, Object> prev, Map<String, Object> cur) {
        for (Map.Entry<String, Object> e : prev.entrySet()) {
            Object before = e.getValue();
            Object after = cur.get(e.getKey());
            if (after == null) continue;
            if (before instanceof Number && after instanceof Number) {
                if (((Number) after).doubleValue() < ((Number) before).doubleValue()) return true;
            } else if (before instanceof String && after instanceof String) {
                if (((String) after).compareTo((String) before) < 0) return true;
            }
        }
        return false;
    }

    private static List<SourceRecord> safePoll(SourceTask task, Result result) {
        try {
            List<SourceRecord> records = task.poll();
            return records == null ? Collections.emptyList() : records;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            result.failures.add("poll() interrupted: " + ie.getMessage());
            return null;
        } catch (Exception ex) {
            result.failures.add("poll() threw " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
            return null;
        }
    }

    private static SourceTask instantiateTask(String connector) throws Exception {
        String fqn = "io.kafka.connect.generated." + connector + "SourceTask";
        Class<?> cls = Class.forName(fqn);
        return (SourceTask) cls.getDeclaredConstructor().newInstance();
    }

    private static SourceTaskContext contextFor(InMemoryOffsets offsets) {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Collections.emptyMap();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return offsets;
            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null;
            }
        };
    }

    /** In-memory offset reader that doubles as a snapshotter. Thread-safe is not required for serial harness. */
    private static final class InMemoryOffsets implements OffsetStorageReader {

        private final Map<Map<String, Object>, Map<String, Object>> store = new LinkedHashMap<>();

        void update(Map<String, ?> partition, Map<String, ?> offset) {
            store.put(copy(partition), copy(offset));
        }

        void bulkLoad(Map<Map<String, Object>, Map<String, Object>> data) {
            data.forEach((k, v) -> store.put(copy(k), copy(v)));
        }

        Map<Map<String, Object>, Map<String, Object>> snapshot() {
            Map<Map<String, Object>, Map<String, Object>> out = new LinkedHashMap<>();
            store.forEach((k, v) -> out.put(copy(k), copy(v)));
            return out;
        }

        @Override
        public <T> Map<String, Object> offset(Map<String, T> partition) {
            Map<String, Object> probe = copy(partition);
            return store.getOrDefault(probe, null);
        }

        @Override
        public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
            Map<Map<String, T>, Map<String, Object>> out = new LinkedHashMap<>();
            for (Map<String, T> p : partitions) {
                Map<String, Object> probe = copy(p);
                Map<String, Object> off = store.get(probe);
                if (off != null) out.put(p, off);
            }
            return out;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static Map<String, Object> copy(Map<String, ?> in) {
            return new LinkedHashMap<>((Map) in);
        }
    }

    /** Outcome of one phase. {@code skipped} short-circuits assertions; {@code failures} drives them. */
    public static final class Result {
        public boolean skipped;
        public String skipReason;
        public final List<String> failures = new ArrayList<>();
        public final Map<String, Integer> recordCounts = new LinkedHashMap<>();
        public Map<Map<String, Object>, Map<String, Object>> finalOffsets = Collections.emptyMap();

        static Result skipped(String reason) {
            Result r = new Result();
            r.skipped = true;
            r.skipReason = reason;
            return r;
        }

        public boolean ok() {
            return !skipped && failures.isEmpty();
        }

        public String summary() {
            if (skipped) return "SKIPPED: " + skipReason;
            if (failures.isEmpty()) return "OK records=" + recordCounts;
            return "FAILED records=" + recordCounts + " failures=" + failures;
        }
    }
}
