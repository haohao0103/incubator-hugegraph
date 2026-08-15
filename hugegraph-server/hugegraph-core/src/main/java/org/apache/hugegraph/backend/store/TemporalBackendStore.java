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

package org.apache.hugegraph.backend.store;

import java.util.List;

import org.apache.hugegraph.temporal.TemporalQuery;
import org.apache.hugegraph.temporal.store.TemporalErrorCode;
import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.temporal.store.TemporalIntervalResult;
import org.apache.hugegraph.temporal.store.TemporalStoreException;
import org.apache.hugegraph.temporal.store.TemporalWrite;

/**
 * Capability sub-interface for backends that support temporal graphs.
 *
 * It deliberately adds NO abstract method to {@link BackendStore}: a backend
 * opts in by implementing this interface, and every non-temporal backend keeps
 * its ordinary graph read/write path unchanged. Temporal writes must enter only
 * through a transaction that checks this capability first (see
 * {@link #require(BackendStore)}); a non-capable backend must receive an
 * explicit not-supported error, never a silent scan or downgrade.
 *
 * The {@link TemporalWrite.Request} carried by {@link #temporalMutate} is the
 * canonical store-facing command: the transaction validates the interval, fact
 * key, idempotency key and closed-state against the frozen Phase 0 contract,
 * and the backend assembles the four views (history / current / open-interval
 * index / temporal index) and applies them in one committed revision. The final
 * idempotency, conflict and open-index decisions remain on the Store apply path.
 */
public interface TemporalBackendStore extends BackendStore {

    /** Whether this store supports temporal graphs. Implementors opt in. */
    default boolean supportsTemporal() {
        return true;
    }

    /**
     * Submit one temporal mutation inside the current transaction boundary.
     * The store must apply the four views in the same committed revision as any
     * ordinary mutation submitted in the same transaction (design ruling §2.5).
     */
    void temporalMutate(TemporalWrite.Request request);

    /**
     * Fact-scoped temporal read ({@code as_of} / {@code between} /
     * {@code overlap}). The fact key identifies the fact sequence; the returned
     * intervals carry only the valid-time window and committed revision, the
     * caller already holds the entity label / temporal label / granularity.
     */
    List<TemporalIntervalResult> temporalQuery(TemporalFactKey factKey, TemporalQuery query);

    /**
     * Cast {@code store} to a temporal-capable store or fail explicitly.
     *
     * @throws TemporalStoreException with {@link TemporalErrorCode#TEMPORAL_UNSUPPORTED_VERSION}
     *         when the backend does not implement this capability
     */
    static TemporalBackendStore require(BackendStore store) {
        if (store instanceof TemporalBackendStore) {
            return (TemporalBackendStore) store;
        }
        throw new TemporalStoreException(
                TemporalErrorCode.TEMPORAL_UNSUPPORTED_VERSION,
                "backend '" + (store == null ? "null" : store.store()) +
                "' does not support temporal graphs");
    }

    /** Whether {@code store} implements the temporal capability. */
    static boolean isTemporal(BackendStore store) {
        return store instanceof TemporalBackendStore;
    }
}
