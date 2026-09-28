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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Single source of truth for the Phase C element-index layout
 * ({@code g+temporal_element_index}): {@code element_id -> its valid intervals},
 * so a reader can decide in one seek whether a bound graph element is valid at a
 * time {@code T} without walking the whole fact sequence.
 *
 * <pre>
 *   key   = element_index_version || element_kind || len_prefixed(element_id)
 *           || sortable(valid_from) || sortable(valid_to)
 *   value = big-endian(committed_revision) || state || fact_key
 * </pre>
 *
 * <p>The value reuses {@link TemporalIntervalCodec#value} (revision || state ||
 * payload) with the canonical fact key as the payload, so a reader that lands on
 * an element-index entry can hop straight back to the fact sequence. The key
 * prefix {@code version || kind || len_prefixed(element_id)} is a stable seekable
 * prefix for "every interval of this element"; the length prefix guarantees two
 * different element ids can never share a prefix. The fixed-width sortable
 * {@code valid_from} / {@code valid_to} suffix ({@code sortable(long) = long ^
 * Long.MIN_VALUE}, sign-flipped big endian) makes the lexicographic byte order
 * equal the numeric order, including negative epoch millis. An open interval
 * stores {@code valid_to = OPEN_VALID_TO} (Long.MAX_VALUE).</p>
 *
 * <p>Co-location contract: the element-index entry is written under the SAME
 * key-hash code (the fact-key hash) and in the SAME Store transaction as the
 * interval views, so it never crosses a Region relative to its fact sequence and
 * commits atomically with it (Phase B). It is a purely additive table: an
 * unbound temporal write produces no element-index entry, keeping the ordinary
 * path byte-for-byte identical to before Phase C.</p>
 */
public final class TemporalElementIndexCodec {

    /** Element-index key layout version byte; a reader must refuse, never guess. */
    public static final byte ELEMENT_INDEX_VERSION = 1;

    /** Sentinel {@code valid_to} for an open (still valid) interval. */
    public static final long OPEN_VALID_TO = Long.MAX_VALUE;

    private TemporalElementIndexCodec() {
    }

    /** Seekable prefix of every interval of one element: version || kind || id. */
    public static byte[] elementPrefix(byte kindCode, byte[] elementId) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ELEMENT_INDEX_VERSION);
        out.write(kindCode);
        writeLengthPrefixed(out, elementId);
        return out.toByteArray();
    }

    /**
     * Full element-index key for one interval of one element. {@code validTo}
     * uses {@link #OPEN_VALID_TO} for an open interval.
     */
    public static byte[] elementIndexKey(byte kindCode, byte[] elementId,
                                         long validFrom, long validTo) {
        byte[] prefix = elementPrefix(kindCode, elementId);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefix, 0, prefix.length);
        writeSortableLong(out, validFrom);
        writeSortableLong(out, validTo);
        return out.toByteArray();
    }

    /**
     * Parse an element-index key that exactly matches {@code kindCode} +
     * {@code elementId}.
     *
     * @return the decoded interval, or {@code null} when the key belongs to a
     *         different element/kind sharing the same code (a hash collision) or
     *         a different layout version; the caller must skip {@code null}.
     */
    public static ElementInterval parse(byte[] key, byte kindCode, byte[] elementId) {
        byte[] prefix = elementPrefix(kindCode, elementId);
        if (key.length != prefix.length + Long.BYTES * 2) {
            return null;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return null;
            }
        }
        ByteBuffer buffer = ByteBuffer.wrap(key, prefix.length, Long.BYTES * 2);
        long validFrom = readSortableLong(buffer);
        long validTo = readSortableLong(buffer);
        boolean open = validTo == OPEN_VALID_TO;
        return new ElementInterval(validFrom, open ? null : validTo, open);
    }

    private static void writeLengthPrefixed(ByteArrayOutputStream out, byte[] value) {
        int len = value.length;
        out.write((len >>> 24) & 0xFF);
        out.write((len >>> 16) & 0xFF);
        out.write((len >>> 8) & 0xFF);
        out.write(len & 0xFF);
        out.write(value, 0, value.length);
    }

    private static void writeSortableLong(ByteArrayOutputStream out, long value) {
        long v = value ^ Long.MIN_VALUE;
        for (int i = 7; i >= 0; i--) {
            out.write((int) ((v >>> (i * 8)) & 0xFF));
        }
    }

    private static long readSortableLong(ByteBuffer buffer) {
        return buffer.getLong() ^ Long.MIN_VALUE;
    }

    /** Decoded element-index interval. */
    public static final class ElementInterval {

        public final long validFrom;
        /** {@code null} means open (still valid). */
        public final Long validTo;
        public final boolean open;

        ElementInterval(long validFrom, Long validTo, boolean open) {
            this.validFrom = validFrom;
            this.validTo = validTo;
            this.open = open;
        }
    }
}
