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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the four Phase-1 Google Ads requester subclasses. */
class GoogleAdsRequestersTest {

    @Test
    void criterionFullRefreshBuildsSelectFromQuery() {
        CriterionFullRefreshRequester r = new CriterionFullRefreshRequester(
            Collections.emptyMap(),
            Map.of(
                "fields", List.of("id", "name", "change_status.last_change_date_time", "deleted_at"),
                "resource_name", "ad_group_criterion"));

        String q = r.buildQuery(Collections.emptyMap(), Collections.emptyMap());
        assertEquals("SELECT id, name FROM ad_group_criterion", q);
    }

    @Test
    void criterionIncrementalAddsInClauseFromPartition() {
        CriterionIncrementalRequester r = new CriterionIncrementalRequester(
            Collections.emptyMap(),
            Map.of(
                "fields", List.of("id", "name", "change_status.last_change_date_time"),
                "resource_name", "ad_group_criterion",
                "primary_key", List.of("id")));

        Map<String, Object> partition = Map.of("id", List.of("11", "22"));
        String q = r.buildQuery(partition, Collections.emptyMap());
        assertEquals("SELECT id, name FROM ad_group_criterion WHERE id IN ('11', '22')", q);
    }

    @Test
    void criterionIncrementalEmptyIdsProducesEmptyInClause() {
        CriterionIncrementalRequester r = new CriterionIncrementalRequester(
            Collections.emptyMap(),
            Map.of(
                "fields", List.of("id"),
                "resource_name", "ad_group_criterion",
                "primary_key", List.of("id")));

        String q = r.buildQuery(Collections.emptyMap(), Collections.emptyMap());
        assertTrue(q.endsWith("IN ()"));
    }

    @Test
    void changeStatusBuildsBetweenWithResourceType() {
        ChangeStatusRequester r = new ChangeStatusRequester(
            Collections.emptyMap(),
            Map.of(
                "fields", List.of("change_status.resource_name"),
                "name", "change_status",
                "resource_type", "AD_GROUP_CRITERION"));

        Map<String, Object> slice = Map.of("start_time", "2024-01-01", "end_time", "2024-01-02");
        String q = r.buildQuery(Collections.emptyMap(), slice);
        assertTrue(q.contains("WHERE change_status.last_change_date_time BETWEEN '2024-01-01' AND '2024-01-02'"));
        assertTrue(q.contains("AND change_status.resource_type = AD_GROUP_CRITERION"));
        assertTrue(q.endsWith("LIMIT 10000"));
    }

    @Test
    void clickViewBuildsSingleDayQuery() {
        ClickViewHttpRequester r = new ClickViewHttpRequester(
            Collections.emptyMap(),
            Map.of("fields", List.of("click_view.gclid")));

        String q = r.buildQuery(Collections.emptyMap(), Map.of("start_time", "2024-05-05"));
        assertEquals("SELECT click_view.gclid FROM click_view WHERE segments.date = '2024-05-05'", q);
    }

    @Test
    void sendThrowsConnectExceptionInPhase1() {
        ClickViewHttpRequester r = new ClickViewHttpRequester(
            Collections.emptyMap(),
            Map.of("fields", List.of("a")));

        ConnectException ex = assertThrows(ConnectException.class,
            () -> r.send(Collections.emptyMap(), Map.of("start_time", "2024-01-01")));
        assertTrue(ex.getMessage().contains("not yet implemented in Phase 1"));
    }

    @Test
    void nullParamsAcceptedByConstructors() {
        // BaseGoogleAdsHttpRequester normalises nulls to empty maps so subclasses do not NPE.
        new ClickViewHttpRequester(null, null);
        new ChangeStatusRequester(null, null);
        new CriterionFullRefreshRequester(null, null);
        new CriterionIncrementalRequester(null, null);
    }
}
