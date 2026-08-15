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

package org.apache.hugegraph.store.raft;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.HgStoreException;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

/**
 * Raft state machine
 */
@Slf4j
public class PartitionStateMachine extends StateMachineAdapter {

    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private final SnapshotHandler snapshotHandler;
    private final Integer groupId;
    private final List<RaftTaskHandler> taskHandlers;
    private final List<RaftStateListener> stateListeners;

    private final Lock lock = new ReentrantLock();
    // JRaft normally invokes onApply serially, but the Store entry point must
    // make this invariant explicit: a temporal read/decision/write bundle may
    // not overlap another apply on the same partition.
    private final Lock applyLock = new ReentrantLock();
    private long committedIndex;

    public PartitionStateMachine(Integer groupId, SnapshotHandler snapshotHandler) {
        this.groupId = groupId;
        this.snapshotHandler = snapshotHandler;
        this.stateListeners = new CopyOnWriteArrayList<>();
        this.taskHandlers = new CopyOnWriteArrayList<>();
    }

    public void addTaskHandler(RaftTaskHandler handler) {
        taskHandlers.add(handler);
    }

    public void addStateListener(RaftStateListener listener) {
        stateListeners.add(listener);
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(Iterator iter) {
        log.info("temporal apply-loop stateMachineId={} groupId={} thread={} index={}",
                 System.identityHashCode(this), groupId, Thread.currentThread().getName(),
                 iter.getIndex());
        applyLock.lock();
        try {
            while (iter.hasNext()) {
                final DefaultRaftClosure done = (DefaultRaftClosure) iter.done();
                // Instrumentation (Slice 1 condition 3): emit the raft-state fields
                // per applied entry so the verifier can join term / isLeader onto the
                // temporal apply-entry line by (groupId, index). term/isLeader are the
                // discriminating fields for the leader-switch variant: they tell whether
                // a decision was applied on the old or new leader.
                log.info("raft apply context groupId={} index={} term={} isLeader={} " +
                         "stateMachineId={}", groupId, iter.getIndex(), iter.getTerm(),
                         isLeader(), System.identityHashCode(this));
                try {
                boolean handled = false;
                for (RaftTaskHandler handler : taskHandlers) {
                    if (done != null) {
                        // Leader branch, call locally
                        RaftOperation operation = done.getOperation();
                        if (handler.invoke(groupId, operation.getOp(), operation.getReq(),
                                           done.getClosure(), iter.getIndex())) {
                            handled = true;
                            done.run(Status.OK());
                            break;
                        }
                    } else {
                        if (handler.invoke(groupId, iter.getData().array(), null,
                                           iter.getIndex())) {
                            handled = true;
                            break;
                        }
                    }
                }
                if (!handled) {
                    // Wire protocol ruling §5.1: unknown/unhandled op must fail
                    // explicitly instead of being silently skipped. The catch
                    // below records it and the caller observes a failure/timeout.
                    throw new HgStoreException(
                            HgStoreException.EC_TEMPORAL_UNSUPPORTED_VERSION,
                            "no handler for raft op, explicit fail instead of " +
                            "silent skip");
                }
            } catch (Throwable t) {
                // A temporal apply can deterministically REJECT a mutation
                // (interval conflict) or refuse an unsupported wire version.
                // These are EXPECTED business outcomes, decided identically on
                // every replica -- they are NOT StateMachine critical errors and
                // must not be logged as such (Slice 1 replay condition 2: zero
                // critical errors on conflict rounds), nor may the raft entry
                // payload be dumped at INFO (L1a). Classify them and surface a
                // specific raft code (L1b) so the client sees a clean rejection
                // instead of UNKNOWN tripping the getErrorResponse() default.
                RaftClosure closure = (done != null) ? done.getClosure() : null;
                if (t instanceof HgStoreException &&
                    isTemporalBusinessRejection(((HgStoreException) t).getCode())) {
                    HgStoreException e = (HgStoreException) t;
                    log.warn("temporal apply rejected groupId={} index={} code={} msg={}",
                             groupId, iter.getIndex(), e.getCode(), e.getMessage());
                    if (closure != null) {
                        closure.run(new Status(temporalRaftError(e.getCode()).getNumber(),
                                               e.getMessage() + " code=" + e.getCode()));
                    }
                } else {
                    log.error(String.format("StateMachine %s meet critical error:", groupId), t);
                    if (done != null) {
                        log.error("StateMachine meet critical error: op = {} {}.",
                                  done.getOperation().getOp(),
                                  done.getOperation().getReq());
                    }
                    // Explicitly surface apply failures to the caller instead of
                    // letting the request hang until timeout. The Raft log still
                    // advances below.
                    if (closure != null && t instanceof HgStoreException) {
                        HgStoreException e = (HgStoreException) t;
                        closure.run(new Status(HgRaftError.UNKNOWN.getNumber(),
                                               e.getMessage() + " code=" + e.getCode()));
                    } else if (closure != null) {
                        closure.run(new Status(HgRaftError.UNKNOWN.getNumber(),
                                               String.valueOf(t.getMessage())));
                    }
                }
            }
            committedIndex = iter.getIndex();
            stateListeners.forEach(listener -> listener.onDataCommitted(committedIndex));
            // clear data
            if (done != null) {
                done.clear();
            }
            // next entry
                iter.next();
            }
        } finally {
            applyLock.unlock();
        }
    }

    public long getCommittedIndex() {
        return committedIndex;
    }

    public long getLeaderTerm() {
        return leaderTerm.get();
    }

    /**
     * A temporal apply may throw {@link HgStoreException} with a temporal
     * business-rejection code. Such an outcome is deterministic and identical on
     * every replica; it is NOT a StateMachine critical error and must be handled
     * on a distinct, quiet path (Slice 1 replay condition 2 / L1b).
     */
    private static boolean isTemporalBusinessRejection(int code) {
        return code == HgStoreException.EC_TEMPORAL_CONFLICT ||
               code == HgStoreException.EC_TEMPORAL_UNSUPPORTED_VERSION ||
               code == HgStoreException.EC_TEMPORAL_CLOSED_INTERVAL_CONFLICT;
    }

    /** Map a temporal business-rejection code onto its dedicated raft error. */
    private static HgRaftError temporalRaftError(int code) {
        if (code == HgStoreException.EC_TEMPORAL_CONFLICT) {
            return HgRaftError.TEMPORAL_CONFLICT;
        }
        if (code == HgStoreException.EC_TEMPORAL_CLOSED_INTERVAL_CONFLICT) {
            return HgRaftError.TEMPORAL_CLOSED_INTERVAL_CONFLICT;
        }
        return HgRaftError.TEMPORAL_UNSUPPORTED_VERSION;
    }

    @Override
    public void onError(final RaftException e) {
        log.error(String.format("Raft %s StateMachine on error {}", groupId), e);
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> listener.onError(e));
        });
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
        Utils.runInThread(() -> stateListeners.forEach(l -> l.onLeaderStart(term)));
        log.info("Raft {} becomes leader ", groupId);
    }

    @Override
    public void onLeaderStop(final Status status) {
        Utils.runInThread(() -> stateListeners.forEach(l -> l.onLeaderStop(this.leaderTerm.get())));
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        log.info("Raft {} lost leader ", groupId);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        Utils.runInThread(
                () -> stateListeners.forEach(
                        l -> l.onStartFollowing(ctx.getLeaderId(), ctx.getTerm())));
        log.info("Raft {} start following: {}.", groupId, ctx);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        Utils.runInThread(
                () -> stateListeners.forEach(
                        l -> l.onStopFollowing(ctx.getLeaderId(), ctx.getTerm())));
        if (!ctx.getStatus().getRaftError().equals(RaftError.ESHUTDOWN)) {
            log.info("Raft {} stop following: {}.", groupId, ctx);
        }
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        stateListeners.forEach(listener -> {
            Utils.runInThread(() -> {
                try {
                    listener.onConfigurationCommitted(conf);
                } catch (Exception e) {
                    log.error("Raft {} onConfigurationCommitted {}", groupId, e);
                }
            });
        });
        log.info("Raft {} onConfigurationCommitted {}", groupId, conf);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        HgStoreEngine.getUninterruptibleJobs().execute(() -> {
            try {
                lock.lock();
                snapshotHandler.onSnapshotSave(writer);
                log.info("Raft {} onSnapshotSave success", groupId);
                done.run(Status.OK());
            } catch (HgStoreException e) {
                log.error(String.format("Raft %s onSnapshotSave failed. {}", groupId), e);
                done.run(new Status(RaftError.EIO, e.toString()));
            } finally {
                lock.unlock();
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        try {
            RaftOutter.SnapshotMeta meta = reader.load();
            if (meta != null) {
                this.committedIndex = meta.getLastIncludedIndex();
                log.info("onSnapshotLoad committedIndex = {}", this.committedIndex);
            } else {
                log.error("onSnapshotLoad failed to get SnapshotMeta");
                return false;
            }
        } catch (Exception e) {
            log.error("onSnapshotLoad failed to get SnapshotMeta.", e);
            return false;
        }

        if (isLeader()) {
            log.warn("Leader is not supposed to load snapshot");
            return false;
        }
        try {
            snapshotHandler.onSnapshotLoad(reader, this.committedIndex);
            log.info("Raft {} onSnapshotLoad success", groupId);
            return true;
        } catch (HgStoreException e) {
            log.error(String.format("Raft %s  onSnapshotLoad failed. ", groupId), e);
            return false;
        }
    }

}
