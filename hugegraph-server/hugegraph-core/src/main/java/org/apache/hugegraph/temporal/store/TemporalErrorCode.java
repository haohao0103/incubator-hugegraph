/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.temporal.store;

/**
 * Closed set of temporal error codes frozen by Phase 0 contract section 5.3.
 *
 * No synonym or additional temporal error code may be introduced outside this
 * enum. Any new code requires a Phase 0 contract amendment first.
 */
public enum TemporalErrorCode {

    /**
     * Same mutation_id resubmitted with a different canonical fact key,
     * interval or payload.
     */
    IDEMPOTENCY_CONFLICT,

    /**
     * Half-open interval intersection detected on the same fact sequence.
     */
    TEMPORAL_CONFLICT,

    /**
     * current / history / open-index / temporal-index of one fact sequence are
     * not colocated, or one temporal mutation touches multiple Regions.
     */
    TEMPORAL_CROSS_REGION_UNSUPPORTED,

    /**
     * Query walked back beyond as_of.max_buckets or as_of.max_rows, or an
     * entity level fan-out exceeded its mandatory limit.
     */
    TEMPORAL_QUERY_LIMIT_EXCEEDED,

    /**
     * Server / Store does not support the requested temporal schema, row-key
     * or capability version.
     */
    TEMPORAL_UNSUPPORTED_VERSION,

    /**
     * Schema / table discovery found an unknown temporal table family or
     * row-key prefix.
     */
    UNKNOWN_TEMPORAL_SCHEMA,

    /**
     * Upsert hit an already closed interval and tried to modify or reopen it
     * in place.
     */
    TEMPORAL_CLOSED_INTERVAL_CONFLICT,

    /**
     * Colocation group (graph, temporal_label, entity_id, fact_key) reached the
     * single Region row / byte / write-rate limit.
     */
    TEMPORAL_COLOCATION_CAPACITY_EXCEEDED
}
