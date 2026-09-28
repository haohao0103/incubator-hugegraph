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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.core.raft;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.raft.DefaultRaftClosure;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.temporal.TemporalMutationHandler;
import org.junit.Test;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;

/**
 * Regression guard for the Phase A1 change in
 * {@link PartitionStateMachine#onApply(Iterator)}: the temporal wire-protocol
 * handling (explicit fail on an unhandled op and the business-rejection
 * classification) must be scoped to temporal entries only, so the non-temporal
 * apply path keeps its pre-temporal behavior.
 *
 * <p>These tests drive the leader branch (a non-null done closure), where the
 * apply outcome is observable through the closure status:
 * <ul>
 *   <li>a temporal op that no handler claims must surface an explicit failure
 *       (never a silent skip);</li>
 *   <li>a legacy (non-temporal) op that no handler claims must keep the original
 *       silent-skip behavior -- the closure is never run;</li>
 *   <li>a legacy op that a handler claims must still be reported OK.</li>
 * </ul>
 */
public class PartitionStateMachineTemporalScopingTest {

    /** Any op byte other than the temporal marker represents a legacy request. */
    private static final byte LEGACY_OP = (byte) 0x01;

    @Test
    public void temporalUnhandledOpFailsExplicitly() {
        AtomicReference<Status> captured = new AtomicReference<>();
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        // No task handler is registered, so the temporal op is left unhandled.
        byte op = TemporalMutationHandler.TEMPORAL_MUTATION;
        Iterator iter = singleEntryIterator(op, new DefaultRaftClosure(
                RaftOperation.create(op), (RaftClosure) captured::set));

        sm.onApply(iter);

        Status status = captured.get();
        assertNotNull("an unhandled temporal op must surface an explicit failure", status);
        assertFalse("an unhandled temporal op must not be reported OK", status.isOk());
    }

    @Test
    public void nonTemporalUnhandledOpIsSilentlySkipped() {
        AtomicReference<Status> captured = new AtomicReference<>();
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        // No task handler is registered, so the legacy op is left unhandled.
        Iterator iter = singleEntryIterator(LEGACY_OP, new DefaultRaftClosure(
                RaftOperation.create(LEGACY_OP), (RaftClosure) captured::set));

        sm.onApply(iter);

        assertNull("a legacy unhandled op must keep the pre-temporal silent-skip " +
                   "behavior (its closure is never run)", captured.get());
    }

    @Test
    public void nonTemporalHandledOpStillRunsOk() {
        AtomicReference<Status> captured = new AtomicReference<>();
        PartitionStateMachine sm = new PartitionStateMachine(0, null);
        sm.addTaskHandler(new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) {
                return false;
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response) {
                return methodId == LEGACY_OP;
            }
        });
        Iterator iter = singleEntryIterator(LEGACY_OP, new DefaultRaftClosure(
                RaftOperation.create(LEGACY_OP), (RaftClosure) captured::set));

        sm.onApply(iter);

        Status status = captured.get();
        assertNotNull("a handled legacy op must run its closure", status);
        assertTrue("a handled legacy op must be reported OK", status.isOk());
    }

    /**
     * Build a single-entry raft iterator for the leader branch. The entry data is
     * captured up front because {@code onApply} clears the done closure (nulling
     * its operation) before advancing the iterator.
     */
    private static Iterator singleEntryIterator(byte op, DefaultRaftClosure done) {
        final byte[] data = new byte[]{op};
        return new Iterator() {
            private boolean consumed = false;

            @Override
            public ByteBuffer getData() {
                return ByteBuffer.wrap(data);
            }

            @Override
            public long getIndex() {
                return 1L;
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
