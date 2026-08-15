/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.hugegraph.backend.store.hstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.store.temporal.TemporalMutationBundle;
import org.apache.hugegraph.temporal.store.TemporalMutationPlan;
import org.apache.hugegraph.temporal.store.TemporalMutationPlanner;
import org.apache.hugegraph.temporal.store.TemporalRowKeyCodec;
import org.apache.hugegraph.temporal.store.TemporalWrite;

/**
 * Bridge between the Server core planner and the Store bundle.
 *
 * This is the only Server module that sees both the core types
 * ({@link TemporalWrite.Request} / {@link TemporalMutationPlan}) and the Store
 * type ({@link TemporalMutationBundle}). It converts the frozen four-view plan
 * into the Store bundle that the hg-store-client submits over the
 * {@code temporalMutation} RPC.
 *
 * <p>An interval-creating bundle (APPEND/UPSERT) always carries four views
 * (frozen codec constraint): a CLOSED interval has no open-interval index entry
 * (design ruling §3.1), so the open-index view is padded with an empty no-op
 * key that the Store writes but never reads. CLOSE/DELETE carry no interval
 * views; the Store close/delete state machine manipulates the history marker
 * directly.</p>
 */
public final class TemporalMutationBundleFactory {

    private static final byte[] EMPTY = new byte[0];

    private TemporalMutationBundleFactory() {
    }

    public static TemporalMutationBundle build(TemporalWrite.Request request,
                                               TemporalRowKeyCodec codec) {
        return build(request, TemporalMutationPlanner.plan(request, codec));
    }

    public static TemporalMutationBundle build(TemporalWrite.Request request,
                                               TemporalMutationPlan plan) {
        if (request.operation() == TemporalWrite.Operation.CLOSE ||
            request.operation() == TemporalWrite.Operation.DELETE) {
            boolean open = request.validTo() == null;
            return new TemporalMutationBundle(
                    toOperation(request.operation()),
                    request.graphId(),
                    request.temporalLabel(),
                    request.entityId(),
                    request.factKey().canonicalBytes(),
                    request.mutationId(),
                    request.schemaVersion(),
                    request.validFrom(),
                    open ? 0L : request.validTo(),
                    open,
                    request.payload(),
                    Collections.emptyList());
        }

        List<TemporalMutationBundle.ViewMutation> views = new ArrayList<>(4);
        for (TemporalMutationPlan.ViewKey view : plan.views()) {
            views.add(new TemporalMutationBundle.ViewMutation(view.view(), view.key(),
                                                              EMPTY));
        }
        // Frozen codec requires exactly four views; pad a closed interval's
        // absent open-index view with an empty no-op key.
        if (plan.view(TemporalMutationPlanner.OPEN_INDEX_VIEW) == null) {
            views.add(new TemporalMutationBundle.ViewMutation(
                    TemporalMutationPlanner.OPEN_INDEX_VIEW, EMPTY, EMPTY));
        }

        boolean open = request.validTo() == null;
        return new TemporalMutationBundle(
                toOperation(request.operation()),
                request.graphId(),
                request.temporalLabel(),
                request.entityId(),
                request.factKey().canonicalBytes(),
                request.mutationId(),
                request.schemaVersion(),
                request.validFrom(),
                open ? 0L : request.validTo(),
                open,
                request.payload(),
                views);
    }

    private static TemporalMutationBundle.Operation toOperation(
            TemporalWrite.Operation operation) {
        switch (operation) {
            case APPEND:
                return TemporalMutationBundle.Operation.APPEND;
            case UPSERT:
                return TemporalMutationBundle.Operation.UPSERT;
            case CLOSE:
                return TemporalMutationBundle.Operation.CLOSE;
            case DELETE:
                return TemporalMutationBundle.Operation.DELETE;
            default:
                throw new IllegalArgumentException("unknown temporal operation: " + operation);
        }
    }
}
