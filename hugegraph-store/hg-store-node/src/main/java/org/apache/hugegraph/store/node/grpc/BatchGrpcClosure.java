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

package org.apache.hugegraph.store.node.grpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.ResStatus;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.PartitionFaultResponse;
import org.apache.hugegraph.store.grpc.session.PartitionFaultType;
import org.apache.hugegraph.store.grpc.session.PartitionLeader;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.util.HgRaftError;

import com.alipay.sofa.jraft.Status;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * Batch processing grpc callback wrapper class
 *
 * @param <V>
 */
@Slf4j
class BatchGrpcClosure<V> {

    private final CountDownLatch countDownLatch;
    private final List<Status> errorStatus;
    private final List<V> results;
    private final Map<Integer, Long> leaderMap;
    /**
     * Number of raft callbacks this batch expects. Needed by waitFinish() to tell
     * "legitimately empty batch" apart from "no callback ever arrived" (W-1).
     */
    private final int expectedCount;

    public BatchGrpcClosure(int count) {
        expectedCount = count;
        countDownLatch = new CountDownLatch(count);
        errorStatus = Collections.synchronizedList(new ArrayList<>());
        results = Collections.synchronizedList(new ArrayList<>());
        leaderMap = new ConcurrentHashMap<>();
    }

    public RaftClosure newRaftClosure() {
        return new GrpcClosure<V>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    results.add(this.getResult());
                } else {
                    leaderMap.putAll(this.getLeaderMap());
                    errorStatus.add(status);
                }
                // Publish the result only after status/result collections are updated.
                // Otherwise waitFinish() can wake up and observe a false success.
                countDownLatch.countDown();
            }
        };
    }

    public RaftClosure newRaftClosure(Consumer<Status> ok) {
        return new GrpcClosure<V>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    results.add(this.getResult());
                } else {
                    leaderMap.putAll(this.getLeaderMap());
                    errorStatus.add(status);
                }
                countDownLatch.countDown();
                ok.accept(status);
            }
        };
    }

    /**
     * Not using counter latch
     *
     * @return
     */
    public RaftClosure newClosureNoLatch() {
        return new GrpcClosure<V>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    results.add(this.getResult());
                } else {
                    leaderMap.putAll(this.getLeaderMap());
                    errorStatus.add(status);
                }
            }
        };
    }

    public PartitionFaultResponse getErrorResponse() {
        PartitionFaultResponse errorResponse;

        if (leaderMap.size() > 0) {
            PartitionFaultResponse.Builder partitionFault =
                    PartitionFaultResponse.newBuilder().setFaultType(
                            PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LEADER);
            leaderMap.forEach((k, v) -> {
                partitionFault.addPartitionLeaders(PartitionLeader.newBuilder()
                                                                  .setPartitionId(k)
                                                                  .setLeaderId(v).build());
            });
            errorResponse = partitionFault.build();
        } else {
            PartitionFaultType faultType = PartitionFaultType.PARTITION_FAULT_TYPE_UNKNOWN;
            switch (HgRaftError.forNumber(errorStatus.get(0).getCode())) {
                case NOT_LEADER:
                    faultType = PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LEADER;
                    break;
                case WAIT_LEADER_TIMEOUT:
                    faultType = PartitionFaultType.PARTITION_FAULT_TYPE_WAIT_LEADER_TIMEOUT;
                    break;
                case NOT_LOCAL:
                    faultType = PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LOCAL;
                    break;
                case TEMPORAL_CONFLICT:
                case TEMPORAL_UNSUPPORTED_VERSION:
                case TEMPORAL_CLOSED_INTERVAL_CONFLICT:
                    // Slice 1 L1b: a temporal business rejection is not a
                    // partition fault. There is no dedicated PartitionFaultType,
                    // so keep UNKNOWN, but do NOT emit an ERROR "Unmatchable
                    // errorStatus": the rejection is an expected, deterministic
                    // outcome whose detail is already carried in getErrorMsg().
                    break;
                default:
                    log.error("Unmatchable errorStatus: " + errorStatus);
            }
            errorResponse = PartitionFaultResponse.newBuilder().setFaultType(faultType).build();
        }
        return errorResponse;
    }

    public String getErrorMsg() {
        StringBuilder builder = new StringBuilder();
        errorStatus.forEach(status -> {
            if (!status.isOk()) {
                builder.append(status.getErrorMsg());
                builder.append("\n");
            }
        });
        return builder.toString();
    }

    /**
     * Wait for the raft execution to complete, return the result to grpc
     * <p>
     * W-1: the return value of {@link CountDownLatch#await(long, TimeUnit)} used to be
     * discarded. On timeout {@code errorStatus} is still empty, so the request fell into
     * the "no error" branch and {@code selectError(emptyList)} translated it into
     * RES_CODE_OK -- a write whose commit state is unknown was reported to the client as
     * a success (fail-open). A timeout must now produce an explicit failure.
     */
    public void waitFinish(StreamObserver<V> observer, Function<List<V>, V> ok, long timeout) {
        try {
            boolean completed = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            int received = results.size() + errorStatus.size();

            if (!completed || received < expectedCount) {
                // Either the latch timed out, or it was released without every raft
                // callback having published its outcome. In both cases the commit state
                // is UNKNOWN and must never be reported as OK.
                String msg = String.format(
                        "TIMEOUT: raft batch did not finish within %d ms, expected %d " +
                        "callback(s) but received %d (ok=%d, error=%d); commit state is " +
                        "UNKNOWN, the request was NOT confirmed committed.%s",
                        timeout, expectedCount, received, results.size(), errorStatus.size(),
                        errorStatus.isEmpty() ? "" : " Partial errors: " + getErrorMsg());
                log.error("waitFinish timeout: {}", msg);
                observer.onNext((V) FeedbackRes.newBuilder()
                                               .setStatus(ResStatus.newBuilder()
                                                                   .setCode(ResCode.RES_CODE_FAIL)
                                                                   .setMsg(msg))
                                               .build());
            } else if (errorStatus.isEmpty()) {  // No error, merge results
                observer.onNext(ok.apply(results));
            } else {
                observer.onNext((V) FeedbackRes.newBuilder()
                                               .setStatus(ResStatus.newBuilder()
                                                                   .setCode(ResCode.RES_CODE_FAIL)
                                                                   .setMsg(getErrorMsg()))
                                               .setPartitionFaultResponse(this.getErrorResponse())
                                               .build());
            }
        } catch (InterruptedException e) {
            // Restore the interrupt flag: swallowing it hides cancellation from callers.
            Thread.currentThread().interrupt();
            log.error("waitFinish exception: ", e);
            observer.onNext((V) FeedbackRes.newBuilder()
                                           .setStatus(ResStatus.newBuilder()
                                                               .setCode(ResCode.RES_CODE_FAIL)
                                                               .setMsg("INTERRUPTED: " +
                                                                       e.getLocalizedMessage())
                                                               .build()).build());
        }
        observer.onCompleted();
    }

    /**
     * Select one incorrect result from multiple results, if there are no errors, return the
     * first one.
     * <p>
     * Null-element semantics (D-2). {@code results} is only ever appended inside the
     * {@code status.isOk()} branch of {@link #newRaftClosure()}. A null element therefore
     * carries an unambiguous meaning: the raft callback fired with an OK status -- the entry
     * IS committed -- but the apply handler published no payload via
     * {@link GrpcClosure#setResult}. The temporal apply path never sets one. Such a result
     * must be reported as success: turning a raft-confirmed commit into RES_CODE_FAIL is a
     * fail-closed false negative, the mirror image of the W-1 fail-open defect. Only the
     * "no callback at all" case (handled in the empty/expectedCount branches below and in
     * waitFinish) leaves the commit state genuinely UNKNOWN.
     */
    public FeedbackRes selectError(List<FeedbackRes> results) {
        if (!CollectionUtils.isEmpty(results)) {
            AtomicReference<FeedbackRes> res = new AtomicReference<>(null);
            AtomicInteger committedWithoutPayload = new AtomicInteger();
            results.forEach(e -> {
                try {
                    if (e == null) {
                        // Raft-confirmed commit with no payload -- see javadoc above.
                        committedWithoutPayload.incrementAndGet();
                        return;
                    }
                    if (res.get() == null) {
                        res.set(e);
                    }
                    if (e.getStatus().getCode() != ResCode.RES_CODE_OK) {
                        res.set(e);
                    }
                } catch (Exception ex) {
                    log.error("selectError failed to inspect a batch result", ex);
                }
            });
            if (res.get() != null) {
                return res.get();
            }
            if (committedWithoutPayload.get() == results.size()) {
                log.debug("selectError: {} raft callback(s) confirmed OK without a payload",
                          committedWithoutPayload.get());
                return okRes();
            }
            return unknownResultRes("UNAVAILABLE: batch of " + results.size() +
                                    " raft result(s) contained no usable status");
        } else if (expectedCount == 0) {
            // Legitimately empty batch: nothing was ever submitted to raft, so there is
            // nothing that could have failed.
            return okRes();
        } else {
            // W-1 second ring: expectedCount > 0 but not a single raft result arrived.
            // Returning RES_CODE_OK here is what turned a timeout into a false success.
            return unknownResultRes("UNAVAILABLE: expected " + expectedCount +
                                    " raft result(s) but received none; commit state is " +
                                    "UNKNOWN, the request was NOT confirmed committed");
        }
    }

    private FeedbackRes okRes() {
        return FeedbackRes.newBuilder()
                          .setStatus(ResStatus.newBuilder()
                                              .setCode(ResCode.RES_CODE_OK).build())
                          .build();
    }

    private FeedbackRes unknownResultRes(String msg) {
        log.error("selectError: {}", msg);
        return FeedbackRes.newBuilder()
                          .setStatus(ResStatus.newBuilder()
                                              .setCode(ResCode.RES_CODE_FAIL)
                                              .setMsg(msg).build())
                          .build();
    }
}
