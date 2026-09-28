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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Phase B guardrail for the batch co-location rule: a bound temporal bundle may
 * only ride a batch that already writes a normal entry to the SAME partition, so
 * the normal write and its validity interval share one Raft proposal. Pure unit
 * test over the extracted {@link HgStoreSessionImpl#findCrossRegionTemporalPartition}
 * seam; it also pins the non-temporal path (no bundles -&gt; never rejected).
 *
 * <p>Lives in {@code hg-store-node} (not {@code hg-store-test}) because the seam
 * is package-private on {@link HgStoreSessionImpl}; {@code hg-store-node} is
 * repackaged as a Spring Boot fat jar, so its classes are not consumable as a
 * compile dependency from another module.
 */
public class HgStoreSessionCrossRegionTest {

    @Test
    public void shouldAcceptCoLocatedTemporalPartition() {
        Set<Integer> normal = new LinkedHashSet<>(Arrays.asList(1, 2));
        Set<Integer> temporal = new LinkedHashSet<>(Collections.singletonList(2));
        assertNull(HgStoreSessionImpl.findCrossRegionTemporalPartition(normal, temporal));
    }

    @Test
    public void shouldRejectCrossRegionTemporalPartition() {
        Set<Integer> normal = new LinkedHashSet<>(Arrays.asList(1, 2));
        Set<Integer> temporal = new LinkedHashSet<>(Collections.singletonList(3));
        assertEquals(Integer.valueOf(3),
                     HgStoreSessionImpl.findCrossRegionTemporalPartition(normal, temporal));
    }

    @Test
    public void shouldNeverRejectNonTemporalBatch() {
        // Non-temporal path: no bound bundles -> the co-location rule is a no-op
        // and the batch behaves exactly as before Phase B, regardless of the
        // normal-write partitions (including an empty batch).
        Set<Integer> normal = new LinkedHashSet<>(Arrays.asList(1, 2));
        assertNull(HgStoreSessionImpl.findCrossRegionTemporalPartition(
                normal, Collections.emptySet()));
        assertNull(HgStoreSessionImpl.findCrossRegionTemporalPartition(
                Collections.emptySet(), Collections.emptySet()));
    }
}
