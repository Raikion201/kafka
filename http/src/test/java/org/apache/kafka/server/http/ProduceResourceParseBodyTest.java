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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the body-parsing logic in {@link ProduceResource#parseBody}.
 */
public class ProduceResourceParseBodyTest {

    private static ProduceBody parseBody(String raw) throws Exception {
        Method m = ProduceResource.class.getDeclaredMethod("parseBody", String.class);
        m.setAccessible(true);
        return (ProduceBody) m.invoke(null, raw);
    }

    @Test
    public void nullBodyProducesTombstone() throws Exception {
        ProduceBody body = parseBody(null);
        assertNull(body.key());
        assertNull(body.value());
        assertNull(body.schemaId());
    }

    @Test
    public void blankBodyProducesTombstone() throws Exception {
        ProduceBody body = parseBody("   ");
        assertNull(body.key());
        assertNull(body.value());
        assertNull(body.schemaId());
    }

    @Test
    public void validJsonBodyIsDeserialised() throws Exception {
        ProduceBody body = parseBody("{\"key\":\"k1\",\"value\":\"v1\"}");
        assertEquals("k1", body.key());
        assertEquals("v1", body.value());
        assertNull(body.schemaId());
    }

    @Test
    public void jsonWithSchemaIdIsDeserialised() throws Exception {
        ProduceBody body = parseBody("{\"key\":\"k1\",\"value\":\"v1\",\"schemaId\":42}");
        assertEquals("k1", body.key());
        assertEquals("v1", body.value());
        assertEquals(42, body.schemaId());
    }

    @Test
    public void jsonWithValueOnlyIsDeserialised() throws Exception {
        ProduceBody body = parseBody("{\"value\":\"hello\"}");
        assertNull(body.key());
        assertEquals("hello", body.value());
        assertNull(body.schemaId());
    }

    @Test
    public void rawStringBodyIsUsedAsValue() throws Exception {
        ProduceBody body = parseBody("hello from kafka connect");
        assertNull(body.key());
        assertEquals("hello from kafka connect", body.value());
        assertNull(body.schemaId());
    }

    @Test
    public void rawStringBodyIsTrimmed() throws Exception {
        ProduceBody body = parseBody("  trimmed  ");
        assertEquals("trimmed", body.value());
    }

    @Test
    public void invalidJsonFallsBackToRawValue() throws Exception {
        ProduceBody body = parseBody("{not valid json}");
        assertNull(body.key());
        assertEquals("{not valid json}", body.value());
        assertNull(body.schemaId());
    }
}
