/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.temporal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.temporal.TemporalQuery;
import org.apache.hugegraph.temporal.TemporalTime;
import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.temporal.store.TemporalIntervalResult;
import org.apache.hugegraph.temporal.store.TemporalWrite;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

/**
 * Independent temporal REST resource (Phase 5). Fact-scoped append / upsert /
 * close / delete and as_of / between / overlap. The fact_key is a JSON object
 * of schema dimensions; the server rebuilds the canonical bytes from the
 * dimension order, never from a client supplied encoding.
 */
@Path("graphspaces/{graphspace}/graphs/{graph}/intervals")
@Singleton
@Tag(name = "TemporalAPI")
public class TemporalAPI extends API {

    @POST
    @Timed(name = "temporal-append")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$graph $action=vertex_write"})
    public Map<String, Object> append(@Context GraphManager manager,
                                      @PathParam("graphspace") String graphSpace,
                                      @PathParam("graph") String graph,
                                      JsonTemporalMutation mutation) {
        checkMutation(mutation);

        HugeGraph g = graph(manager, graphSpace, graph);
        TemporalWrite.Operation op = TemporalWrite.Operation.valueOf(
                mutation.operation.toUpperCase());
        TemporalFactKey factKey = TemporalFactKey.of(
                new ArrayList<>(mutation.factKey.keySet()), mutation.factKey);
        byte[] payload = mutation.payload == null ? new byte[0]
                                                  : mutation.payload.getBytes();
        long validFrom = TemporalTime.parse(mutation.validFrom);
        Long validTo = mutation.validTo == null ? null
                                                : TemporalTime.parse(mutation.validTo);
        TemporalWrite.Request request = new TemporalWrite.Request(
                op, graph, mutation.temporalLabel, mutation.entityId, factKey,
                validFrom, validTo, payload, mutation.mutationId,
                TemporalSchemaVersionHolder.SCHEMA_VERSION);

        commit(g, () -> {
            g.temporalMutate(request);
            return null;
        });

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entity_id", mutation.entityId);
        result.put("temporal_label", mutation.temporalLabel);
        result.put("mutation_id", mutation.mutationId);
        result.put("operation", mutation.operation);
        return result;
    }

    @GET
    @Timed(name = "temporal-query")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$graph $action=vertex_read"})
    public List<TemporalIntervalResult> query(@Context GraphManager manager,
                                              @PathParam("graphspace") String graphSpace,
                                              @PathParam("graph") String graph,
                                              @QueryParam("temporal_label") String temporalLabel,
                                              @QueryParam("type") String type,
                                              @QueryParam("from") String from,
                                              @QueryParam("to") String to,
                                              @QueryParam("fact_key") String factKey) {
        E.checkArgument(temporalLabel != null && !temporalLabel.isEmpty(),
                        "The temporal_label can't be null or empty");
        E.checkArgument(factKey != null && !factKey.isEmpty(),
                        "The fact_key can't be null or empty");
        E.checkArgument(from != null && !from.isEmpty(),
                        "The from time can't be null or empty");
        long fromTime = TemporalTime.parse(from);
        Long toTime = (to == null || to.isEmpty()) ? null : TemporalTime.parse(to);
        // The fact_key is a JSON object of schema dimensions; parse it and
        // rebuild the canonical bytes from its dimension order.
        @SuppressWarnings("unchecked")
        Map<String, String> dimensions = JsonUtil.fromJson(factKey, Map.class);
        TemporalFactKey parsed = TemporalFactKey.of(
                new ArrayList<>(dimensions.keySet()), dimensions);

        TemporalQuery query;
        switch (type) {
            case "as_of":
                query = TemporalQuery.asOf(fromTime);
                break;
            case "between":
                query = TemporalQuery.between(fromTime, toTime);
                break;
            case "overlap":
                query = TemporalQuery.overlap(fromTime, toTime);
                break;
            default:
                throw new IllegalArgumentException("unknown temporal query type: " + type);
        }

        HugeGraph g = graph(manager, graphSpace, graph);
        return g.temporalQuery(parsed, query);
    }

    private static void checkMutation(JsonTemporalMutation mutation) {
        E.checkNotNull(mutation, "mutation");
        E.checkArgument(mutation.entityId != null && !mutation.entityId.isEmpty(),
                        "The entity_id can't be null or empty");
        E.checkArgument(mutation.temporalLabel != null && !mutation.temporalLabel.isEmpty(),
                        "The temporal_label can't be null or empty");
        E.checkArgument(mutation.factKey != null && !mutation.factKey.isEmpty(),
                        "The fact_key can't be null or empty");
        E.checkArgument(mutation.mutationId != null && !mutation.mutationId.isEmpty(),
                        "The mutation_id can't be null or empty");
        E.checkArgument(mutation.validFrom != null, "The valid_from can't be null");
        E.checkArgument("append".equals(mutation.operation) ||
                        "upsert".equals(mutation.operation) ||
                        "close".equals(mutation.operation) ||
                        "delete".equals(mutation.operation),
                        "The operation must be append, upsert, close or delete");
    }

    public static class JsonTemporalMutation {

        @JsonProperty("entity_id")
        public String entityId;
        @JsonProperty("temporal_label")
        public String temporalLabel;
        @JsonProperty("fact_key")
        public Map<String, String> factKey;
        @JsonProperty("valid_from")
        public Object validFrom;
        @JsonProperty("valid_to")
        public Object validTo;
        @JsonProperty("mutation_id")
        public String mutationId;
        @JsonProperty("payload")
        public String payload;
        @JsonProperty("operation")
        public String operation;
    }

    /** Schema version carried by temporal mutations (wire protocol). */
    private static final class TemporalSchemaVersionHolder {

        static final int SCHEMA_VERSION = 1;
    }
}
