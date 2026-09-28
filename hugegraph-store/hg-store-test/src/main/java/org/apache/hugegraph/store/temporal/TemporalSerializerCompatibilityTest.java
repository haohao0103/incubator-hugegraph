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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the fail-closed serializer compatibility contract defined by the
 * Temporal Serializer Compatibility RFC (docs/TEMPORAL_SERIALIZER_COMPATIBILITY_RFC.md):
 * every temporal byte layer carries an independent version tag and a reader that
 * does not recognize the version must refuse explicitly, never guess, never
 * silently skip and never misparse the bytes as another layout.
 *
 * <p>Covers: bundle {@code CODEC_VERSION} rejection (foreign / older / zero),
 * structural integrity (trailing / truncated bytes), version-registry mirror
 * consistency ({@link TemporalWireProtocol} vs the constants it mirrors), and
 * row-key marker / element-index refusal of a foreign version byte.</p>
 */
public class TemporalSerializerCompatibilityTest {

    // ---- bundle payload: fail-closed exact CODEC_VERSION match ----

    @Test
    public void shouldRejectForeignBundleCodecVersion() throws Exception {
        byte[] encoded = encodedCloseBundle();
        assertBundleVersionRejected(withCodecVersion(encoded,
                                                    TemporalMutationBundle.CODEC_VERSION + 1));
        assertBundleVersionRejected(withCodecVersion(encoded,
                                                    TemporalMutationBundle.CODEC_VERSION - 1));
        assertBundleVersionRejected(withCodecVersion(encoded, 0));
    }

    @Test
    public void shouldRejectTrailingBytes() throws Exception {
        byte[] encoded = encodedCloseBundle();
        byte[] withTrailing = Arrays.copyOf(encoded, encoded.length + 1);
        try {
            TemporalMutationBundleCodec.decode(withTrailing);
            fail("expected IOException for trailing bytes");
        } catch (IOException e) {
            assertTrue("unexpected message: " + e.getMessage(),
                       e.getMessage().contains("trailing bytes"));
        }
    }

    @Test
    public void shouldRejectTruncatedBundle() throws Exception {
        byte[] encoded = encodedCloseBundle();
        // Cut mid-stream: a length-prefixed field runs past the available bytes.
        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 3);
        try {
            TemporalMutationBundleCodec.decode(truncated);
            fail("expected IOException for a truncated bundle");
        } catch (IOException expected) {
            // truncated / invalid field length / EOF are all explicit refusals
        }
    }

    // ---- version registry mirror must never drift from the real constants ----

    @Test
    public void shouldKeepWireProtocolRegistryConsistent() {
        assertEquals(TemporalMutationBundle.CODEC_VERSION, TemporalWireProtocol.CODEC_VERSION);
        assertEquals(TemporalMutationHandler.TEMPORAL_MUTATION, TemporalWireProtocol.WIRE_OP);
        String describe = TemporalWireProtocol.describe();
        assertTrue(describe.contains("codecVersion=" + TemporalWireProtocol.CODEC_VERSION));
        assertTrue(describe.contains("schemaVersion=" + TemporalWireProtocol.SCHEMA_VERSION));
        assertTrue(describe.contains("wireOp=0x" +
                                     Integer.toHexString(TemporalWireProtocol.WIRE_OP & 0xFF)));
    }

    // ---- Store row-key marker: refuse a foreign MARKER_VERSION, never misparse ----

    @Test
    public void shouldRefuseForeignIntervalMarkerVersion() {
        byte[] factKey = bytes("fact-1");
        byte[] key = TemporalIntervalCodec.intervalKey(factKey, 1000L, 2000L);
        // sanity: the untampered key parses back to the same interval
        TemporalIntervalCodec.Interval parsed = TemporalIntervalCodec.parse(key, factKey);
        assertNotNull(parsed);
        assertEquals(1000L, parsed.validFrom);
        assertEquals(Long.valueOf(2000L), parsed.validTo);

        // tamper the version byte that sits right after the fact_key prefix
        byte[] foreign = key.clone();
        foreign[factKey.length] = (byte) (TemporalIntervalCodec.MARKER_VERSION + 1);
        assertNull("a foreign marker version must be refused, not misparsed",
                   TemporalIntervalCodec.parse(foreign, factKey));
    }

    // ---- element-index key: refuse a foreign ELEMENT_INDEX_VERSION ----

    @Test
    public void shouldRefuseForeignElementIndexVersion() {
        byte kindCode = (byte) 1;
        byte[] elementId = bytes("vertex-1");
        byte[] key = TemporalElementIndexCodec.elementIndexKey(kindCode, elementId, 1000L, 2000L);
        // sanity: the untampered key parses back
        assertNotNull(TemporalElementIndexCodec.parse(key, kindCode, elementId));

        byte[] foreign = key.clone();
        foreign[0] = (byte) (TemporalElementIndexCodec.ELEMENT_INDEX_VERSION + 1);
        assertNull("a foreign element-index version must be refused",
                   TemporalElementIndexCodec.parse(foreign, kindCode, elementId));
    }

    // ---- helpers ----

    private static byte[] encodedCloseBundle() throws IOException {
        TemporalMutationBundle bundle = new TemporalMutationBundle(
                TemporalMutationBundle.Operation.CLOSE, "hugegraph",
                "driver_order_rel", "driver_1001", bytes("fact-1"), "m-close", 1,
                100L, 200L, false, new byte[0], Collections.emptyList());
        return TemporalMutationBundleCodec.encode(bundle);
    }

    private static void assertBundleVersionRejected(byte[] encoded) {
        try {
            TemporalMutationBundleCodec.decode(encoded);
            fail("expected IOException for a foreign bundle codec version");
        } catch (IOException e) {
            assertTrue("unexpected message: " + e.getMessage(),
                       e.getMessage().contains("unsupported temporal bundle version"));
        }
    }

    /** Overwrite the big-endian int codec version carried in the first 4 bytes. */
    private static byte[] withCodecVersion(byte[] encoded, int version) {
        byte[] copy = encoded.clone();
        copy[0] = (byte) ((version >>> 24) & 0xFF);
        copy[1] = (byte) ((version >>> 16) & 0xFF);
        copy[2] = (byte) ((version >>> 8) & 0xFF);
        copy[3] = (byte) (version & 0xFF);
        return copy;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
