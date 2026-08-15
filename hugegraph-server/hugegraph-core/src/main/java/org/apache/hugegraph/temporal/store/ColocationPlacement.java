/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.temporal.store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Placement contract of the skeleton: every view of one colocation group
 * (current row, history rows, open interval index, temporal index) resolves to
 * exactly one Region. This class only models the invariant so it can be tested
 * without a cluster; the real placement is owned by PD.
 */
public class ColocationPlacement {

    /** Views that must land in the same Region for a single mutation. */
    public enum View {
        CURRENT,
        HISTORY,
        OPEN_INTERVAL_INDEX,
        TEMPORAL_INDEX
    }

    private final int regionCount;
    private final Map<String, Integer> overrides = new HashMap<>();

    public ColocationPlacement(int regionCount) {
        if (regionCount < 1) {
            throw new IllegalArgumentException("regionCount must be >= 1");
        }
        this.regionCount = regionCount;
    }

    /**
     * Region of one view of a group. All views resolve identically unless a
     * test explicitly injects a split placement to exercise
     * TEMPORAL_CROSS_REGION_UNSUPPORTED.
     */
    public int regionOf(ColocationGroup group, View view) {
        Integer override = this.overrides.get(overrideKey(group, view));
        if (override != null) {
            return override;
        }
        return baseRegionOf(group);
    }

    public int baseRegionOf(ColocationGroup group) {
        byte[] digest = TieBreakers.sha256(TieBreakers.concat(
                group.graphId().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                group.temporalLabel().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                group.entityId().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                group.factKey().canonicalBytes()));
        int v = ((digest[0] & 0xFF) << 24) | ((digest[1] & 0xFF) << 16) |
                ((digest[2] & 0xFF) << 8) | (digest[3] & 0xFF);
        return Math.floorMod(v, this.regionCount);
    }

    /**
     * Test / fault injection hook: force one view of a group onto another
     * Region so the store must reject the whole mutation rather than commit
     * partially or write asynchronously across Regions.
     */
    public void injectSplitPlacement(ColocationGroup group, View view, int regionId) {
        this.overrides.put(overrideKey(group, view), regionId);
    }

    /** All views of the group, used by the pre-commit placement check. */
    public View[] views() {
        return View.values();
    }

    public int regionCount() {
        return this.regionCount;
    }

    private static String overrideKey(ColocationGroup group, View view) {
        return group.toString() + "#" + view.name() +
               "#" + Arrays.hashCode(group.factKey().canonicalBytes());
    }
}
