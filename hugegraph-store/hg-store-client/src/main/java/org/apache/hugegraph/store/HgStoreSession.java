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

package org.apache.hugegraph.store;

import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.grpc.session.TemporalQueryRes;
import org.apache.hugegraph.store.grpc.session.TemporalQueryType;

public interface HgStoreSession extends HgKvStore {

    void beginTx();

    /**
     * @throws IllegalStateException  when the tx hasn't been beginning.
     * @throws HgStoreClientException when failed to commit .
     */
    void commit();

    /**
     * @throws IllegalStateException  when the tx hasn't been beginning.
     * @throws HgStoreClientException when failed to rollback.
     */
    void rollback();

    boolean isTx();

    /**
     * Submit one temporal mutation bundle to the Store over the internal
     * {@code temporalMutation} RPC. {@code code} is the fact-key hash used to
     * resolve the owning partition. The transport (partition resolution and
     * gRPC routing) is wired in the concrete gRPC session; the default keeps
     * other session implementations honest by failing explicitly instead of
     * silently dropping the mutation.
     *
     * @return true when the Store accepted the mutation
     */
    default boolean temporalMutation(int code, byte[] bundle) {
        throw new UnsupportedOperationException(
                "temporalMutation transport is not wired for " +
                getClass().getName());
    }

    /**
     * Fact-scoped temporal read. The fact-key bytes are the scan prefix; the
     * Store derives the owning partition from calcHashcode(fact_key). The
     * default keeps other session implementations honest by failing explicitly.
     */
    default TemporalQueryRes temporalQuery(byte[] factKey, TemporalQueryType type,
                                           long from, long to) {
        throw new UnsupportedOperationException(
                "temporalQuery transport is not wired for " +
                getClass().getName());
    }
}
