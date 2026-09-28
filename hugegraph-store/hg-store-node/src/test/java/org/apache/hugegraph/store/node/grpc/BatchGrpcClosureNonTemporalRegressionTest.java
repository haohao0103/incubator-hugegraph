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

package org.apache.hugegraph.store.node.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.ResStatus;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.util.HgRaftError;
import org.junit.Test;

import com.alipay.sofa.jraft.Status;

import io.grpc.stub.StreamObserver;

/**
 * Regression guard for the shared (non-temporal) gRPC batch write path in
 * {@link BatchGrpcClosure}. The temporal work hardened this class (W-1
 * fail-open -&gt; fail-closed on timeout, latch ordering, D-2 null-payload
 * semantics). Those are correct orthogonal fixes, but they touch the path every
 * ordinary store write flows through, so this test pins the observable
 * non-temporal contract:
 *
 * <ul>
 *   <li>happy path: all callbacks OK -&gt; the merged OK result is returned
 *       (unchanged);</li>
 *   <li>explicit error: an error callback -&gt; RES_CODE_FAIL carrying the error
 *       (unchanged);</li>
 *   <li>W-1: a batch that never receives its callbacks must fail CLOSED, never be
 *       reported as a false success (this is the one intentional delta vs the
 *       pre-temporal fail-open behavior);</li>
 *   <li>D-2: a raft-confirmed commit that published no payload (null element) is
 *       a success, not a fail-closed false negative;</li>
 *   <li>a legitimately empty batch (expectedCount == 0) is still OK.</li>
 * </ul>
 *
 * <p>Lives in {@code hg-store-node} (not {@code hg-store-test}) because it
 * exercises the package-private {@link BatchGrpcClosure}; {@code hg-store-node}
 * is repackaged as a Spring Boot fat jar, so its classes are not consumable as a
 * compile dependency from another module.
 */
public class BatchGrpcClosureNonTemporalRegressionTest {

    @Test
    public void allOkCallbacksMergeIntoOkResult() {
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(2);
        RaftClosure first = closure.newRaftClosure();
        RaftClosure second = closure.newRaftClosure();
        GrpcClosure.setResult(first, okFeedback("r1"));
        GrpcClosure.setResult(second, okFeedback("r2"));
        first.run(Status.OK());
        second.run(Status.OK());

        RecordingObserver observer = new RecordingObserver();
        closure.waitFinish(observer, closure::selectError, 5_000L);

        assertEquals("happy path must merge into a single OK result",
                     ResCode.RES_CODE_OK, observer.last.getStatus().getCode());
        assertTrue("waitFinish must complete the stream", observer.completed);
    }

    @Test
    public void explicitErrorCallbackSurfacesFail() {
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(1);
        RaftClosure raftClosure = closure.newRaftClosure();
        raftClosure.run(new Status(HgRaftError.UNKNOWN.getNumber(), "boom"));

        RecordingObserver observer = new RecordingObserver();
        closure.waitFinish(observer, closure::selectError, 5_000L);

        assertEquals("an explicit raft error must be surfaced as FAIL",
                     ResCode.RES_CODE_FAIL, observer.last.getStatus().getCode());
        assertTrue("the error message must reach the client",
                   observer.last.getStatus().getMsg().contains("boom"));
    }

    @Test
    public void timeoutWithoutCallbacksFailsClosedNotOpen() {
        // expectedCount > 0 but no raft callback ever arrives: the commit state is
        // UNKNOWN. The pre-temporal code reported this as RES_CODE_OK (fail-open,
        // silent data loss); the hardened path must report FAIL.
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(1);

        RecordingObserver observer = new RecordingObserver();
        closure.waitFinish(observer, closure::selectError, 50L);

        assertEquals("a timed-out batch must fail closed, never a false success",
                     ResCode.RES_CODE_FAIL, observer.last.getStatus().getCode());
        assertTrue("waitFinish must still complete the stream", observer.completed);
    }

    @Test
    public void nullPayloadCommitIsReportedOk() {
        // D-2: results only ever receives an element inside the status.isOk()
        // branch, so a null element means "raft confirmed the commit but the apply
        // handler published no payload" (the temporal apply path). That is a
        // success, not a fail-closed false negative.
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(1);
        FeedbackRes selected = closure.selectError(
                new ArrayList<>(Collections.singletonList(null)));

        assertEquals("a raft-confirmed commit without payload must be OK",
                     ResCode.RES_CODE_OK, selected.getStatus().getCode());
    }

    @Test
    public void emptyBatchSemanticsHonorExpectedCount() {
        // A legitimately empty batch (nothing submitted to raft) is OK.
        FeedbackRes legitEmpty = new BatchGrpcClosure<FeedbackRes>(0)
                .selectError(Collections.emptyList());
        assertEquals("an empty batch with expectedCount==0 is OK",
                     ResCode.RES_CODE_OK, legitEmpty.getStatus().getCode());

        // W-1 second ring: expectedCount > 0 but not a single result arrived.
        FeedbackRes lostBatch = new BatchGrpcClosure<FeedbackRes>(2)
                .selectError(Collections.emptyList());
        assertEquals("an empty batch with expectedCount>0 must fail closed",
                     ResCode.RES_CODE_FAIL, lostBatch.getStatus().getCode());
    }

    @Test
    public void mixedNullAndStatusPicksUsableStatus() {
        // A batch may mix a payload-less commit (null) with a real error result;
        // the usable non-OK status must win so the error is not masked.
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(2);
        FeedbackRes error = FeedbackRes.newBuilder()
                                       .setStatus(ResStatus.newBuilder()
                                                           .setCode(ResCode.RES_CODE_FAIL)
                                                           .setMsg("real error")
                                                           .build())
                                       .build();
        FeedbackRes selected = closure.selectError(
                new ArrayList<>(Arrays.asList(null, error)));

        assertEquals("a real error must not be masked by a null payload",
                     ResCode.RES_CODE_FAIL, selected.getStatus().getCode());
        assertEquals("real error", selected.getStatus().getMsg());
    }

    private static FeedbackRes okFeedback(String ignored) {
        return FeedbackRes.newBuilder()
                          .setStatus(ResStatus.newBuilder()
                                              .setCode(ResCode.RES_CODE_OK)
                                              .build())
                          .build();
    }

    /** Minimal StreamObserver that records the last value and completion. */
    private static final class RecordingObserver implements StreamObserver<FeedbackRes> {
        private FeedbackRes last;
        private boolean completed;
        private final List<Throwable> errors = new ArrayList<>();

        @Override
        public void onNext(FeedbackRes value) {
            this.last = value;
        }

        @Override
        public void onError(Throwable t) {
            this.errors.add(t);
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}
