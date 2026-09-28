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

package org.apache.hugegraph.store.core.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.raft.DefaultRaftClosure;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftStateListener;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.temporal.TemporalMutationHandler;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Test;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftException;

/**
 * Phase 3 (distributed consistency &amp; fault recovery) unit guard for the
 * {@link PartitionStateMachine#onApply(Iterator)} paths that only appear when a
 * follower replays the committed log or a node re-applies it after a restart.
 *
 * <p>The sibling {@code PartitionStateMachineTemporalScopingTest} pins the
 * LEADER branch (a non-null done closure). This class pins the complementary
 * REPLAY branch (a null done closure, where the op is recovered from the raw
 * entry byte) plus the temporal business-rejection classification, which are
 * the run-free slice of the Raft-replay / leader-switch story:
 * <ul>
 *   <li>on replay, an unhandled temporal op fails explicitly yet the committed
 *       index still advances, so a rejected entry never stalls log replay;</li>
 *   <li>on replay, an unhandled legacy op keeps its silent-skip behavior and the
 *       log still advances;</li>
 *   <li>on replay, a temporal entry is dispatched to the handler with the exact
 *       committed index (the mechanism that re-applies mutations after a
 *       restart);</li>
 *   <li>a deterministic business rejection surfaces its dedicated raft code
 *       (not UNKNOWN), while a genuine critical failure surfaces UNKNOWN;</li>
 *   <li>replay advances through a batch that contains a rejected entry.</li>
 * </ul>
 * These are handler/state-machine level invariants only; they are NOT evidence
 * of a real HStore/PD/Raft leader switch, restart or snapshot/restore, which
 * the design doc (§6.3, §7.2) requires on a live cluster.
 */
public class PartitionStateMachineTemporalReplayTest {

    /** Any op byte other than the temporal marker represents a legacy request. */
    private static final byte LEGACY_OP = (byte) 0x01;

    @Test
    public void replayUnhandledTemporalOpFailsExplicitlyButAdvancesLog() {
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        AtomicLong committed = new AtomicLong(-1L);
        AtomicInteger fires = new AtomicInteger(0);
        sm.addStateListener(commitListener(committed, fires));
        // No task handler: on the replay branch the temporal op is unhandled and
        // must fail explicitly, but the committed index must still advance.
        byte op = TemporalMutationHandler.TEMPORAL_MUTATION;
        sm.onApply(replayIterator(new byte[]{op}, 5L));

        assertEquals("replay must advance the committed index past a rejected " +
                     "temporal entry (no stuck log)", 5L, committed.get());
        assertEquals(5L, sm.getCommittedIndex());
        assertEquals(1, fires.get());
    }

    @Test
    public void replayUnhandledLegacyOpIsSilentlySkippedButAdvancesLog() {
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        AtomicLong committed = new AtomicLong(-1L);
        AtomicInteger fires = new AtomicInteger(0);
        sm.addStateListener(commitListener(committed, fires));
        // No task handler: a legacy op on the replay branch keeps the
        // pre-temporal silent-skip behavior; the log still advances.
        sm.onApply(replayIterator(new byte[]{LEGACY_OP}, 6L));

        assertEquals(6L, committed.get());
        assertEquals(6L, sm.getCommittedIndex());
        assertEquals(1, fires.get());
    }

    @Test
    public void replayDispatchesTemporalEntryToHandlerWithCommittedIndex() {
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        AtomicLong seenIndex = new AtomicLong(-1L);
        sm.addTaskHandler(new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) {
                return false;
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response) {
                return false;
            }

            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response,
                                  long applyIndex) {
                if (request.length > 0 &&
                    request[0] == TemporalMutationHandler.TEMPORAL_MUTATION) {
                    seenIndex.set(applyIndex);
                    return true;
                }
                return false;
            }
        });
        byte op = TemporalMutationHandler.TEMPORAL_MUTATION;
        sm.onApply(replayIterator(new byte[]{op}, 99L));

        // The replay path must hand the raw entry to the temporal handler with
        // the exact committed index -- this is how a restarted node re-applies.
        assertEquals(99L, seenIndex.get());
        assertEquals(99L, sm.getCommittedIndex());
    }

    @Test
    public void leaderBusinessRejectionSurfacesDedicatedRaftCode() {
        AtomicReference<Status> captured = new AtomicReference<>();
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        sm.addTaskHandler(new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) {
                return false;
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response)
                    throws HgStoreException {
                if (methodId == TemporalMutationHandler.TEMPORAL_MUTATION) {
                    throw new HgStoreException(HgStoreException.EC_TEMPORAL_CONFLICT,
                                               "TEMPORAL_CONFLICT for fact key");
                }
                return false;
            }
        });
        byte op = TemporalMutationHandler.TEMPORAL_MUTATION;
        sm.onApply(leaderIterator(op, new DefaultRaftClosure(
                RaftOperation.create(op), (RaftClosure) captured::set), 1L));

        Status status = captured.get();
        assertNotNull("a business rejection must run the closure", status);
        assertFalse(status.isOk());
        // L1b: a deterministic conflict must carry its dedicated raft code, not
        // UNKNOWN (which would trip the getErrorResponse() default branch).
        assertEquals(HgRaftError.TEMPORAL_CONFLICT.getNumber(), status.getCode());
        assertEquals("the raft log still advances on a business rejection",
                     1L, sm.getCommittedIndex());
    }

    @Test
    public void leaderCriticalTemporalErrorSurfacesUnknown() {
        AtomicReference<Status> captured = new AtomicReference<>();
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        sm.addTaskHandler(new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) {
                return false;
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response)
                    throws HgStoreException {
                if (methodId == TemporalMutationHandler.TEMPORAL_MUTATION) {
                    // Not in the business-rejection set: a genuine apply failure.
                    throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                               "corrupt temporal payload");
                }
                return false;
            }
        });
        byte op = TemporalMutationHandler.TEMPORAL_MUTATION;
        sm.onApply(leaderIterator(op, new DefaultRaftClosure(
                RaftOperation.create(op), (RaftClosure) captured::set), 1L));

        Status status = captured.get();
        assertNotNull(status);
        assertFalse(status.isOk());
        assertEquals("a non-business temporal failure must surface UNKNOWN",
                     HgRaftError.UNKNOWN.getNumber(), status.getCode());
        assertTrue(status.getCode() != HgRaftError.TEMPORAL_CONFLICT.getNumber());
    }

    @Test
    public void replayAdvancesThroughBatchContainingRejectedEntry() {
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        AtomicLong committed = new AtomicLong(-1L);
        AtomicInteger fires = new AtomicInteger(0);
        sm.addStateListener(commitListener(committed, fires));
        // No handler: entries 5 and 7 are rejected temporal ops, entry 6 is a
        // silently-skipped legacy op. Replay must advance through all three.
        byte temporal = TemporalMutationHandler.TEMPORAL_MUTATION;
        sm.onApply(replayIterator(new byte[]{temporal, LEGACY_OP, temporal}, 5L));

        assertEquals("replay must not stall on a rejected mid-log entry",
                     7L, committed.get());
        assertEquals(7L, sm.getCommittedIndex());
        assertEquals("the commit listener fires once per replayed entry", 3, fires.get());
    }

    // ------------------------------------------------------------------ helpers

    private static RaftStateListener commitListener(AtomicLong committed, AtomicInteger fires) {
        return new RaftStateListener() {
            @Override
            public void onLeaderStart(long newTerm) {
                // not exercised by these apply-path tests
            }

            @Override
            public void onError(RaftException e) {
                // not exercised by these apply-path tests
            }

            @Override
            public void onDataCommitted(long index) {
                committed.set(index);
                fires.incrementAndGet();
            }
        };
    }

    /**
     * A follower/restart replay iterator: {@code done()} is null, so the op is
     * recovered from the raw entry byte and the outcome is observable only
     * through the committed index and the state listener. Entries are indexed
     * {@code firstIndex .. firstIndex + n - 1}.
     */
    private static Iterator replayIterator(byte[] ops, long firstIndex) {
        return new Iterator() {
            private int cursor = 0;

            @Override
            public ByteBuffer getData() {
                return ByteBuffer.wrap(new byte[]{ops[this.cursor]});
            }

            @Override
            public long getIndex() {
                return firstIndex + this.cursor;
            }

            @Override
            public long getTerm() {
                return 1L;
            }

            @Override
            public Closure done() {
                return null;
            }

            @Override
            public void setErrorAndRollback(long ntail, Status st) {
                // no-op for the test
            }

            @Override
            public boolean hasNext() {
                return this.cursor < ops.length;
            }

            @Override
            public ByteBuffer next() {
                byte[] data = new byte[]{ops[this.cursor]};
                this.cursor++;
                return ByteBuffer.wrap(data);
            }
        };
    }

    /**
     * A leader-branch iterator (non-null done closure) for a single entry, where
     * the apply outcome is observable through the closure status. The entry data
     * is captured up front because {@code onApply} clears the done closure
     * before advancing the iterator.
     */
    private static Iterator leaderIterator(byte op, DefaultRaftClosure done, long index) {
        final byte[] data = new byte[]{op};
        return new Iterator() {
            private boolean consumed = false;

            @Override
            public ByteBuffer getData() {
                return ByteBuffer.wrap(data);
            }

            @Override
            public long getIndex() {
                return index;
            }

            @Override
            public long getTerm() {
                return 1L;
            }

            @Override
            public Closure done() {
                return done;
            }

            @Override
            public void setErrorAndRollback(long ntail, Status st) {
                // no-op for the test
            }

            @Override
            public boolean hasNext() {
                return !this.consumed;
            }

            @Override
            public ByteBuffer next() {
                this.consumed = true;
                return ByteBuffer.wrap(data);
            }
        };
    }
}
