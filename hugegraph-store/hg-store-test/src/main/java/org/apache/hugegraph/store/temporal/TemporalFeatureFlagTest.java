/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hugegraph.store.temporal;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 6 (rollout / monitoring / release) guard for the temporal write feature
 * flag and, critically, its rollback-safety boundary.
 *
 * <p>{@link TemporalFeatureFlag} gates NEW temporal submissions at the Store RPC
 * entry ({@code HgStoreSessionImpl.temporalMutation} rejects with an explicit
 * status and {@code HgStoreNodeService.addTemporalRaftTask} refuses to propose).
 * It must NOT gate the Raft apply / replay path: a node rolled back to flag-off
 * still has to replay already committed temporal log entries on restart, and a
 * follower still has to apply entries the leader proposed, otherwise the
 * replicas diverge. These tests pin the flag flip contract and that boundary at
 * the unit level; they are NOT a rolling-upgrade or rollback drill on a real
 * HStore/PD/Raft cluster (design doc §6.3, §7.2), which stays cluster-bound.
 */
public class TemporalFeatureFlagTest {

    @After
    public void restoreFlag() {
        // The flag is process-global static state; leave it disabled (its
        // default) so it never leaks into another test in the suite.
        TemporalFeatureFlag.disable();
    }

    @Test
    public void shouldFlipGlobalFlag() {
        TemporalFeatureFlag.disable();
        assertFalse(TemporalFeatureFlag.isEnabled());
        TemporalFeatureFlag.enable();
        assertTrue(TemporalFeatureFlag.isEnabled());
        TemporalFeatureFlag.disable();
        assertFalse(TemporalFeatureFlag.isEnabled());
    }

    @Test
    public void replayIsNotGatedByDisabledFlag() throws Exception {
        // Rollback / restart safety: with the flag explicitly OFF, the apply
        // path must still run. This models a follower or a restarted node
        // re-applying a committed temporal entry (here an idempotent ledger-hit
        // replay). If the flag ever gated apply, this would fail instead of
        // returning true -- which is exactly the divergence we must prevent.
        TemporalFeatureFlag.disable();
        assertFalse(TemporalFeatureFlag.isEnabled());

        BusinessHandler business = mock(BusinessHandler.class);
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getDbPath()).thenReturn("/tmp/temporal-flag-test");
        when(business.getSession(anyInt())).thenReturn(session);
        // The ledger already carries this mutation id -> idempotent replay no-op,
        // which returns before any write, so no tx/scan mocks are needed.
        when(business.doGet(anyString(), anyInt(), anyString(), any()))
                .thenReturn(new byte[0]);

        TemporalMutationHandler handler = new TemporalMutationHandler(business);
        TemporalMutationBundle bundle = new TemporalMutationBundle(
                TemporalMutationBundle.Operation.CLOSE, "g", "label", "entity",
                bytes("fact"), "m-replay", 1, 10L, 20L, false, bytes("payload"),
                Collections.emptyList());

        boolean applied = handler.invoke(7, TemporalMutationHandler.TEMPORAL_MUTATION,
                                         bundle, null, 41L);
        assertTrue("the feature flag must not block replay of a committed entry",
                   applied);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
