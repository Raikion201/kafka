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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.googleads;

import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1.5 stubs must (a) construct cleanly so the registrar can wire them and
 * (b) throw a clear {@link ConnectException} from the not-yet-implemented method.
 * Phase 2 will replace the bodies with the google-ads-java SDK calls and these
 * tests will be rewritten against real wire behaviour.
 */
class GoogleAdsPhase15StubsTest {

    private static final Map<String, String> NO_CONFIG = Collections.emptyMap();
    private static final Map<String, Object> NO_PARAMS = Collections.emptyMap();

    @Test
    void googleAdsHttpRequester_sendThrows() {
        GoogleAdsHttpRequester r = new GoogleAdsHttpRequester(NO_CONFIG, NO_PARAMS);
        ConnectException ex = assertThrows(ConnectException.class,
            () -> r.send(NO_PARAMS, NO_PARAMS));
        assertTrue(ex.getMessage().contains("not yet implemented"),
            "expected Phase-1 message; got: " + ex.getMessage());
    }

    @Test
    void customGAQueryHttpRequester_buildQueryReadsParam() {
        CustomGAQueryHttpRequester r = new CustomGAQueryHttpRequester(
            NO_CONFIG, Map.of("query", "SELECT campaign.id FROM campaign"));
        assertTrue(r.buildQuery(NO_PARAMS, NO_PARAMS).startsWith("SELECT campaign.id"));
    }

    @Test
    void googleAdsStreamingDecoder_decodeThrows() {
        GoogleAdsStreamingDecoder d = new GoogleAdsStreamingDecoder(NO_CONFIG, NO_PARAMS);
        ConnectException ex = assertThrows(ConnectException.class,
            () -> d.decode(new ByteArrayInputStream(new byte[0])));
        assertTrue(ex.getMessage().contains("not yet implemented"));
    }

    @Test
    void googleAdsRetriever_readThrows() {
        GoogleAdsRetriever r = new GoogleAdsRetriever(NO_CONFIG, NO_PARAMS);
        ConnectException ex = assertThrows(ConnectException.class,
            () -> r.read(NO_PARAMS, NO_PARAMS));
        assertTrue(ex.getMessage().contains("not yet implemented"));
    }

    @Test
    void criterionRetriever_readThrows() {
        CriterionRetriever r = new CriterionRetriever(NO_CONFIG, NO_PARAMS);
        ConnectException ex = assertThrows(ConnectException.class,
            () -> r.read(NO_PARAMS, NO_PARAMS));
        assertTrue(ex.getMessage().contains("not yet implemented"));
    }
}
