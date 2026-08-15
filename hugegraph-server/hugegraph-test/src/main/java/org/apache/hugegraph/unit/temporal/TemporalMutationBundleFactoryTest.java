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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.store.hstore.TemporalMutationBundleFactory;
import org.apache.hugegraph.store.temporal.TemporalMutationBundle;
import org.apache.hugegraph.store.temporal.TemporalMutationBundleCodec;
import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.temporal.store.TemporalMutationPlanner;
import org.apache.hugegraph.temporal.store.TemporalRowKeyCodec;
import org.apache.hugegraph.temporal.store.TemporalWrite;
import org.junit.Assert;
import org.junit.Test;

/**
 * Bridge test: Server core {@link TemporalWrite.Request} -&gt; Store
 * {@link TemporalMutationBundle}. Cluster-independent; it verifies the bundle
 * assembly + codec round-trip only.
 */
public class TemporalMutationBundleFactoryTest {

    private static final String GRAPH = "hugegraph";
    private static final String LABEL = "driver_order_rel";
    private static final String ENTITY = "driver_1001";
    private static final List<String> DIMS =
            Arrays.asList("subject_id", "object_id", "relation_type");

    private static final TemporalRowKeyCodec CODEC = new TemporalRowKeyCodec();

    private static TemporalFactKey fact() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("subject_id", "d1");
        values.put("object_id", "o1");
        values.put("relation_type", "drives");
        return TemporalFactKey.of(DIMS, values);
    }

    @Test
    public void shouldBuildOpenBundleWithFourViews() {
        TemporalFactKey fk = fact();
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 100L, null, "payload", "m1");
        TemporalMutationBundle bundle =
                TemporalMutationBundleFactory.build(req, CODEC);

        Assert.assertTrue(bundle.open());
        Assert.assertEquals(4, bundle.views().size());
        Assert.assertArrayEquals(fk.canonicalBytes(), bundle.factKey());
        Assert.assertEquals("m1", bundle.mutationId());
        Assert.assertEquals(1, bundle.schemaVersion());
        Assert.assertEquals(100L, bundle.validFrom());
        assertViewNames(bundle, 4);
    }

    @Test
    public void shouldBuildClosedBundleWithNoopOpenIndex() {
        TemporalFactKey fk = fact();
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "payload", "m1");
        TemporalMutationBundle bundle =
                TemporalMutationBundleFactory.build(req, CODEC);

        Assert.assertFalse(bundle.open());
        Assert.assertEquals(200L, bundle.validTo());
        Assert.assertEquals(4, bundle.views().size());
        // A closed interval has no open-index entry; the frozen codec still
        // requires four views, so it is padded with an empty no-op key.
        TemporalMutationBundle.ViewMutation openIndex =
                viewOf(bundle, TemporalMutationPlanner.OPEN_INDEX_VIEW);
        Assert.assertNotNull(openIndex);
        Assert.assertEquals(0, openIndex.key().length);
    }

    @Test
    public void shouldRoundTripCodec() throws Exception {
        TemporalFactKey fk = fact();
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "payload", "m1");
        TemporalMutationBundle source =
                TemporalMutationBundleFactory.build(req, CODEC);

        byte[] encoded = TemporalMutationBundleCodec.encode(source);
        TemporalMutationBundle decoded = TemporalMutationBundleCodec.decode(encoded);

        Assert.assertEquals(source.graph(), decoded.graph());
        Assert.assertEquals(source.temporalLabel(), decoded.temporalLabel());
        Assert.assertEquals(source.entityId(), decoded.entityId());
        Assert.assertArrayEquals(source.factKey(), decoded.factKey());
        Assert.assertEquals(source.mutationId(), decoded.mutationId());
        Assert.assertEquals(source.schemaVersion(), decoded.schemaVersion());
        Assert.assertEquals(source.validFrom(), decoded.validFrom());
        Assert.assertEquals(source.validTo(), decoded.validTo());
        Assert.assertEquals(source.open(), decoded.open());
        Assert.assertArrayEquals(source.payload(), decoded.payload());
        Assert.assertEquals(4, decoded.views().size());
    }

    @Test
    public void shouldBuildCloseBundleWithZeroViews() {
        TemporalFactKey fk = fact();
        TemporalWrite.Request req = TemporalWrite.Request.close(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "m1");
        TemporalMutationBundle bundle =
                TemporalMutationBundleFactory.build(req, CODEC);

        Assert.assertEquals(TemporalMutationBundle.Operation.CLOSE,
                            bundle.operation());
        Assert.assertEquals(0, bundle.views().size());
        Assert.assertEquals(200L, bundle.validTo());
        Assert.assertFalse(bundle.open());
    }

    @Test
    public void shouldBuildDeleteBundleWithZeroViews() {
        TemporalFactKey fk = fact();
        TemporalWrite.Request req = TemporalWrite.Request.delete(
                GRAPH, LABEL, ENTITY, fk, 100L, "m1");
        TemporalMutationBundle bundle =
                TemporalMutationBundleFactory.build(req, CODEC);

        Assert.assertEquals(TemporalMutationBundle.Operation.DELETE,
                            bundle.operation());
        Assert.assertEquals(0, bundle.views().size());
    }

    @Test
    public void shouldRoundTripCloseDeleteCodec() throws Exception {
        TemporalFactKey fk = fact();
        TemporalWrite.Request close = TemporalWrite.Request.close(
                GRAPH, LABEL, ENTITY, fk, 100L, 200L, "m1");
        TemporalMutationBundle source =
                TemporalMutationBundleFactory.build(close, CODEC);

        byte[] encoded = TemporalMutationBundleCodec.encode(source);
        TemporalMutationBundle decoded = TemporalMutationBundleCodec.decode(encoded);

        Assert.assertEquals(source.operation(), decoded.operation());
        Assert.assertEquals(0, decoded.views().size());
        Assert.assertEquals(200L, decoded.validTo());
    }

    private static void assertViewNames(TemporalMutationBundle bundle, int count) {
        Assert.assertEquals(count, bundle.views().size());
        Assert.assertNotNull(viewOf(bundle, TemporalMutationPlanner.HISTORY_VIEW));
        Assert.assertNotNull(viewOf(bundle, TemporalMutationPlanner.CURRENT_VIEW));
        Assert.assertNotNull(viewOf(bundle, TemporalMutationPlanner.INDEX_VIEW));
    }

    private static TemporalMutationBundle.ViewMutation viewOf(TemporalMutationBundle bundle,
                                                              String name) {
        for (TemporalMutationBundle.ViewMutation view : bundle.views()) {
            if (view.name().equals(name)) {
                return view;
            }
        }
        return null;
    }
}
