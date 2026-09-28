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

package org.apache.hugegraph.store.temporal;

import org.apache.hugegraph.store.business.BusinessHandlerAtomicBatchTest;
import org.apache.hugegraph.store.core.raft.PartitionStateMachineTemporalReplayTest;
import org.apache.hugegraph.store.core.raft.PartitionStateMachineTemporalScopingTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Always-on regression suite for the Store-side temporal read/write path.
 *
 * <p>Only tests that run free (no live PD/Store/Raft cluster) belong here, so
 * the suite is safe to wire into the default {@code mvn test} lifecycle. The
 * interval codec / mutation handler / query handler cover the fact-scoped
 * bucketed layout and its bounded-scan guards; the feature-flag test pins the
 * rollout gate and its replay-ungated rollback boundary; the raft-scoping,
 * raft-replay and atomic-batch tests pin the "explicit fail instead of silent
 * skip", replay-branch log advancement / rejection classification and
 * same-transaction invariants. Cluster-dependent integration tests
 * ({@code TemporalStoreDirect*Test}) are deliberately excluded: they require a
 * real HStore/PD/Raft deployment and are validated in the Phase 2/3 cluster
 * runs, not in unit regression.</p>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TemporalMutationBundleCodecTest.class,
        TemporalFeatureFlagTest.class,
        TemporalMutationHandlerTest.class,
        TemporalQueryHandlerTest.class,
        PartitionStateMachineTemporalScopingTest.class,
        PartitionStateMachineTemporalReplayTest.class,
        BusinessHandlerAtomicBatchTest.class
})
public class TemporalSuiteTest {

}
