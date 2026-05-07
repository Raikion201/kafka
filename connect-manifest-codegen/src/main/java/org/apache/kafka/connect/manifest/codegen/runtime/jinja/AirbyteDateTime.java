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

import org.apache.kafka.connect.manifest.codegen.runtime.DatetimeWindowHelper;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

/**
 * Wrapper around {@link ZonedDateTime} returned by {@code now_utc()} so that
 * Jinja templates can call Python-style instance methods on it.
 *
 * <p>jinjava resolves template method calls via Java reflection.  Returning a
 * plain {@code String} from {@code now_utc()} means templates like
 * {@code now_utc().strftime('%Y-%m-%d')} and
 * {@code (now_utc() - duration('P1D')).strftime(...)} fail at runtime because
 * {@code String} has no {@code strftime} method.</p>
 *
 * <p>The jinjava binary {@code -} operator does not support arbitrary object
 * subtraction; the template pre-processor in {@link JinjaRenderer} rewrites
 * {@code now_utc() - duration(X)} to {@code now_utc().minus(duration(X))}
 * before handing the template to jinjava, so the subtraction is resolved as a
 * plain method call instead.</p>
 *
 * <p>{@link #toString()} returns an ISO-8601 string so that string contexts
 * (comparisons, assignments) continue to work without modification.</p>
 */
public final class AirbyteDateTime {

    private final ZonedDateTime zdt;

    public AirbyteDateTime(ZonedDateTime zdt) {
        this.zdt = zdt;
    }

    /**
     * Format this datetime using a Python {@code strftime} format string.
     * Delegates to {@link DatetimeWindowHelper#formatDate(ZonedDateTime, String)}.
     */
    public String strftime(String fmt) {
        return DatetimeWindowHelper.formatDate(zdt, fmt);
    }

    /**
     * Subtract a duration and return a new {@link AirbyteDateTime}.
     * Called by the pre-processed form of {@code now_utc() - duration(X)}.
     */
    public AirbyteDateTime minus(Object amount) {
        if (amount instanceof TemporalAmount ta) {
            return new AirbyteDateTime(zdt.minus(ta));
        }
        return this;
    }

    /**
     * Add a duration and return a new {@link AirbyteDateTime}.
     */
    public AirbyteDateTime plus(Object amount) {
        if (amount instanceof TemporalAmount ta) {
            return new AirbyteDateTime(zdt.plus(ta));
        }
        return this;
    }

    /**
     * Expose the underlying {@link ZonedDateTime} for use in
     * {@link AirbyteJinjaFunctions#formatDatetime} and similar helpers.
     */
    public ZonedDateTime toZonedDateTime() {
        return zdt;
    }

    /**
     * ISO-8601 representation so string contexts keep working (e.g. comparisons
     * against date strings, boolean or-chains like
     * {@code config.get('start', now_utc())}).
     */
    @Override
    public String toString() {
        return zdt.withZoneSameInstant(ZoneOffset.UTC)
            .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"));
    }
}
