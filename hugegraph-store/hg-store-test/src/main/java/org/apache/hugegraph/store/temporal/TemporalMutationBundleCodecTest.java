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
package org.apache.hugegraph.store.temporal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TemporalMutationBundleCodecTest {

    @Test
    public void shouldRoundTripVersionedBundle() throws Exception {
        List<TemporalMutationBundle.ViewMutation> views = Arrays.asList(
                new TemporalMutationBundle.ViewMutation("history", bytes("h-key"), bytes("h-value")),
                new TemporalMutationBundle.ViewMutation("current", bytes("c-key"), bytes("c-value")),
                new TemporalMutationBundle.ViewMutation("open_interval_index", bytes("o-key"), bytes("o-value")),
                new TemporalMutationBundle.ViewMutation("temporal_index", bytes("i-key"), bytes("i-value")));
        TemporalMutationBundle source = new TemporalMutationBundle(
                "hugegraph", "driver_order_rel", "driver_1001", bytes("fact-1"),
                "mutation-1", 1, 100L, 200L, false, bytes("payload"), views);

        byte[] encoded = TemporalMutationBundleCodec.encode(source);
        TemporalMutationBundle decoded = TemporalMutationBundleCodec.decode(encoded);

        assertEquals(source.graph(), decoded.graph());
        assertEquals(source.temporalLabel(), decoded.temporalLabel());
        assertEquals(source.entityId(), decoded.entityId());
        assertArrayEquals(source.factKey(), decoded.factKey());
        assertEquals(source.mutationId(), decoded.mutationId());
        assertEquals(source.schemaVersion(), decoded.schemaVersion());
        assertEquals(source.validFrom(), decoded.validFrom());
        assertEquals(source.validTo(), decoded.validTo());
        assertEquals(source.open(), decoded.open());
        assertArrayEquals(source.payload(), decoded.payload());
        assertEquals(4, decoded.views().size());
        for (int i = 0; i < 4; i++) {
            assertEquals(source.views().get(i).name(), decoded.views().get(i).name());
            assertArrayEquals(source.views().get(i).key(), decoded.views().get(i).key());
            assertArrayEquals(source.views().get(i).value(), decoded.views().get(i).value());
        }
    }

    @Test
    public void shouldRoundTripCloseBundle() throws Exception {
        TemporalMutationBundle source = new TemporalMutationBundle(
                TemporalMutationBundle.Operation.CLOSE, "hugegraph",
                "driver_order_rel", "driver_1001", bytes("fact-1"), "m-close", 1,
                100L, 200L, false, new byte[0], Collections.emptyList());

        byte[] encoded = TemporalMutationBundleCodec.encode(source);
        TemporalMutationBundle decoded = TemporalMutationBundleCodec.decode(encoded);

        assertEquals(TemporalMutationBundle.Operation.CLOSE, decoded.operation());
        assertEquals(0, decoded.views().size());
        assertEquals(100L, decoded.validFrom());
        assertEquals(200L, decoded.validTo());
        assertEquals("m-close", decoded.mutationId());
    }

    @Test
    public void shouldRoundTripDeleteBundle() throws Exception {
        TemporalMutationBundle source = new TemporalMutationBundle(
                TemporalMutationBundle.Operation.DELETE, "hugegraph",
                "driver_order_rel", "driver_1001", bytes("fact-1"), "m-delete", 1,
                100L, 0L, true, new byte[0], Collections.emptyList());

        byte[] encoded = TemporalMutationBundleCodec.encode(source);
        TemporalMutationBundle decoded = TemporalMutationBundleCodec.decode(encoded);

        assertEquals(TemporalMutationBundle.Operation.DELETE, decoded.operation());
        assertEquals(0, decoded.views().size());
        assertEquals(100L, decoded.validFrom());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
