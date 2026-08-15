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

package org.apache.hugegraph.unit.temporal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.temporal.store.TemporalMutationPlan;
import org.apache.hugegraph.temporal.store.TemporalMutationPlanner;
import org.apache.hugegraph.temporal.store.TemporalRowKeyCodec;
import org.apache.hugegraph.temporal.store.TemporalWrite;
import org.apache.hugegraph.temporal.store.TieBreakers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Cluster-independent unit test of the Server-side four-view mutation planner.
 *
 * It verifies the frozen view NAMES and the fixed-width KEY layouts only; it is
 * NOT evidence of a real HStore/PD/Raft append result.
 */
public class TemporalMutationPlannerTest {

    private static final String GRAPH = "hugegraph";
    private static final String LABEL = "driver_order_rel";
    private static final String ENTITY = "driver_1001";
    private static final List<String> DIMS =
            Arrays.asList("subject_id", "object_id", "relation_type");

    private static final TemporalRowKeyCodec CODEC = new TemporalRowKeyCodec();

    private static TemporalFactKey fact(String subject, String object,
                                        String relation) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("subject_id", subject);
        values.put("object_id", object);
        values.put("relation_type", relation);
        return TemporalFactKey.of(DIMS, values);
    }

    @Test
    public void shouldProduceFourViewsForOpenAppend() {
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fact("d1", "o1", "drives"), 100L, null,
                "payload", "m1");
        TemporalMutationPlan plan = TemporalMutationPlanner.plan(req, CODEC);

        Assert.assertEquals(4, plan.views().size());
        Assert.assertNotNull(plan.view(TemporalMutationPlanner.HISTORY_VIEW));
        Assert.assertNotNull(plan.view(TemporalMutationPlanner.CURRENT_VIEW));
        Assert.assertNotNull(plan.view(TemporalMutationPlanner.OPEN_INDEX_VIEW));
        Assert.assertNotNull(plan.view(TemporalMutationPlanner.INDEX_VIEW));
    }

    @Test
    public void shouldProduceThreeViewsForClosedAppend() {
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fact("d1", "o1", "drives"), 100L, 200L,
                "payload", "m1");
        TemporalMutationPlan plan = TemporalMutationPlanner.plan(req, CODEC);

        Assert.assertEquals(3, plan.views().size());
        Assert.assertNull(plan.view(TemporalMutationPlanner.OPEN_INDEX_VIEW));
    }

    @Test
    public void shouldDeriveDeterministicTieBreaker() {
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fact("d1", "o1", "drives"), 100L, 200L,
                "payload", "m1");
        TemporalMutationPlan first = TemporalMutationPlanner.plan(req, CODEC);
        TemporalMutationPlan second = TemporalMutationPlanner.plan(req, CODEC);
        Assert.assertEquals(first.tieBreaker(), second.tieBreaker());
        Assert.assertEquals(TieBreakers.LENGTH, first.tieBreaker().length());

        TemporalWrite.Request other = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fact("d1", "o1", "drives"), 100L, 200L,
                "payload", "m2");
        Assert.assertNotEquals(first.tieBreaker(),
                               TemporalMutationPlanner.plan(other, CODEC).tieBreaker());
    }

    @Test
    public void shouldBuildHistoryKeyViaCodec() {
        TemporalFactKey fk = fact("d1", "o1", "drives");
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "payload", "m1");
        TemporalMutationPlan plan = TemporalMutationPlanner.plan(req, CODEC);

        byte[] expected = CODEC.historyRowKey(GRAPH, LABEL, ENTITY, fk, 100L,
                                              plan.tieBreaker());
        Assert.assertArrayEquals(expected,
                                 plan.view(TemporalMutationPlanner.HISTORY_VIEW).key());
    }

    @Test
    public void shouldBuildCurrentKeyWithMarkerAndFactKey() {
        TemporalFactKey fk = fact("d1", "o1", "drives");
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "payload", "m1");
        TemporalMutationPlan plan = TemporalMutationPlanner.plan(req, CODEC);

        byte[] prefix = CODEC.groupPrefix(GRAPH, LABEL, ENTITY, fk);
        byte[] key = plan.view(TemporalMutationPlanner.CURRENT_VIEW).key();
        byte[] canonical = fk.canonicalBytes();

        Assert.assertEquals(prefix.length + 1 + canonical.length, key.length);
        Assert.assertTrue(startsWith(key, prefix));
        Assert.assertEquals(0x43, key[prefix.length] & 0xFF);
        Assert.assertArrayEquals(Arrays.copyOfRange(key, prefix.length + 1,
                                                    key.length), canonical);
    }

    @Test
    public void shouldBuildIndexKeysWithFixedWidthSuffix() {
        TemporalFactKey fk = fact("d1", "o1", "drives");
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 100L, null, "payload", "m1");
        TemporalMutationPlan plan = TemporalMutationPlanner.plan(req, CODEC);

        byte[] prefix = CODEC.groupPrefix(GRAPH, LABEL, ENTITY, fk);
        assertIndexKey(plan.view(TemporalMutationPlanner.OPEN_INDEX_VIEW),
                       prefix, 0x4F, 100L, plan.tieBreaker());
        assertIndexKey(plan.view(TemporalMutationPlanner.INDEX_VIEW),
                       prefix, 0x49, 100L, plan.tieBreaker());
    }

    @Test
    public void shouldMatchStoreTableNames() {
        Assert.assertEquals("g+temporal_history", TemporalMutationPlanner.HISTORY_VIEW);
        Assert.assertEquals("g+temporal_current", TemporalMutationPlanner.CURRENT_VIEW);
        Assert.assertEquals("g+temporal_open_index", TemporalMutationPlanner.OPEN_INDEX_VIEW);
        Assert.assertEquals("g+temporal_index", TemporalMutationPlanner.INDEX_VIEW);
    }

    @Test
    public void shouldProduceEmptyPlanForCloseAndDelete() {
        TemporalFactKey fk = fact("d1", "o1", "drives");
        TemporalWrite.Request close = TemporalWrite.Request.close(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "m1");
        TemporalWrite.Request delete = TemporalWrite.Request.delete(
                GRAPH, LABEL, ENTITY, fk, 100L, "m1");
        // close/delete carry no interval views; the close/delete state machine
        // applies them on the Store path.
        Assert.assertEquals(0, TemporalMutationPlanner.plan(close, CODEC).views().size());
        Assert.assertEquals(0, TemporalMutationPlanner.plan(delete, CODEC).views().size());
    }

    private static void assertIndexKey(TemporalMutationPlan.ViewKey view,
                                       byte[] prefix, int marker, long validFrom,
                                       String tieBreaker) {
        Assert.assertNotNull(view);
        byte[] key = view.key();
        byte[] tie = tieBreaker.getBytes(StandardCharsets.US_ASCII);

        Assert.assertEquals(prefix.length + 1 + 8 + tie.length, key.length);
        Assert.assertTrue(startsWith(key, prefix));
        Assert.assertEquals(marker, key[prefix.length] & 0xFF);
        byte[] fromBytes = Arrays.copyOfRange(key, prefix.length + 1,
                                              prefix.length + 1 + 8);
        Assert.assertArrayEquals(bigEndianLong(validFrom), fromBytes);
        Assert.assertArrayEquals(Arrays.copyOfRange(key, prefix.length + 1 + 8,
                                                    key.length), tie);
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] bigEndianLong(long v) {
        byte[] out = new byte[8];
        for (int i = 7; i >= 0; i--) {
            out[7 - i] = (byte) ((v >>> (i * 8)) & 0xFF);
        }
        return out;
    }
}
