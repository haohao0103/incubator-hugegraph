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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Fixed row key encoder frozen by the design ruling section 2.1:
 *
 * graph_id | temporal_label | entity_id | fact_key_hash | time_bucket |
 * valid_from | tie_breaker
 *
 * Encoding rules:
 * - String components are length prefixed so a separator can never be injected
 *   and two different component splits can never share the same bytes.
 * - fact_key_hash is a versioned fixed width prefix. It is a locator only:
 *   identity comparison must always use the raw canonical fact key.
 * - time_bucket and valid_from are 8 byte sign flipped big endian longs so the
 *   lexicographic byte order equals the numeric order, including negative
 *   epoch millis.
 * - tie_breaker is a fixed width base32 string derived from mutation_id.
 */
public final class TemporalRowKeyCodec {

    public static final byte ROW_KEY_VERSION = 1;
    public static final int DEFAULT_FACT_KEY_HASH_BYTES = 8;

    /** Default valid-time bucket width: 7 days, see design section 2.1. */
    public static final long DEFAULT_BUCKET_WIDTH_MILLIS = 7L * 24 * 3600 * 1000;

    private final int factKeyHashBytes;
    private final long bucketWidthMillis;

    public TemporalRowKeyCodec() {
        this(DEFAULT_FACT_KEY_HASH_BYTES, DEFAULT_BUCKET_WIDTH_MILLIS);
    }

    /**
     * @param factKeyHashBytes width of the fact_key_hash locator; narrowing it
     *                         is only used by tests to force hash collisions
     * @param bucketWidthMillis configurable, not a performance conclusion
     */
    public TemporalRowKeyCodec(int factKeyHashBytes, long bucketWidthMillis) {
        if (factKeyHashBytes < 1 || factKeyHashBytes > 32) {
            throw new IllegalArgumentException(
                    "fact_key_hash width must be in [1, 32]");
        }
        if (bucketWidthMillis <= 0) {
            throw new IllegalArgumentException("bucket width must be positive");
        }
        this.factKeyHashBytes = factKeyHashBytes;
        this.bucketWidthMillis = bucketWidthMillis;
    }

    public int factKeyHashBytes() {
        return this.factKeyHashBytes;
    }

    public long bucketWidthMillis() {
        return this.bucketWidthMillis;
    }

    /** Bucket index derived from valid_from, floor division for negatives. */
    public long bucketOf(long validFromMillis) {
        return Math.floorDiv(validFromMillis, this.bucketWidthMillis);
    }

    public byte[] factKeyHash(TemporalFactKey factKey) {
        byte[] digest = TieBreakers.sha256(factKey.canonicalBytes());
        byte[] hash = new byte[1 + this.factKeyHashBytes];
        // versioned hash encoding
        hash[0] = ROW_KEY_VERSION;
        System.arraycopy(digest, 0, hash, 1, this.factKeyHashBytes);
        return hash;
    }

    /**
     * Colocation group prefix: graph | label | entity | fact_key_hash.
     * All history rows, the current row and the open interval index of one
     * fact sequence share this prefix and never cross a Region.
     */
    public byte[] groupPrefix(String graphId, String temporalLabel,
                              String entityId, TemporalFactKey factKey) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ROW_KEY_VERSION);
        writeString(out, graphId);
        writeString(out, temporalLabel);
        writeString(out, entityId);
        byte[] hash = factKeyHash(factKey);
        out.write(hash, 0, hash.length);
        return out.toByteArray();
    }

    public byte[] historyRowKey(String graphId, String temporalLabel,
                                String entityId, TemporalFactKey factKey,
                                long validFromMillis, String tieBreaker) {
        if (tieBreaker == null || tieBreaker.length() != TieBreakers.LENGTH) {
            throw new IllegalArgumentException(
                    "tie_breaker must be exactly " + TieBreakers.LENGTH +
                    " chars, got: " + tieBreaker);
        }
        byte[] prefix = groupPrefix(graphId, temporalLabel, entityId, factKey);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefix, 0, prefix.length);
        writeSortableLong(out, bucketOf(validFromMillis));
        writeSortableLong(out, validFromMillis);
        byte[] tb = tieBreaker.getBytes(StandardCharsets.US_ASCII);
        out.write(tb, 0, tb.length);
        return out.toByteArray();
    }

    private static void writeString(ByteArrayOutputStream out, String s) {
        byte[] raw = s.getBytes(StandardCharsets.UTF_8);
        out.write((raw.length >>> 24) & 0xFF);
        out.write((raw.length >>> 16) & 0xFF);
        out.write((raw.length >>> 8) & 0xFF);
        out.write(raw.length & 0xFF);
        out.write(raw, 0, raw.length);
    }

    /** Sign flipped big endian long: byte order equals numeric order. */
    static void writeSortableLong(ByteArrayOutputStream out, long value) {
        long v = value ^ Long.MIN_VALUE;
        for (int i = 7; i >= 0; i--) {
            out.write((int) ((v >>> (i * 8)) & 0xFF));
        }
    }

    /** Unsigned lexicographic byte comparison, the Store row order. */
    public static int compare(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int x = a[i] & 0xFF;
            int y = b[i] & 0xFF;
            if (x != y) {
                return x < y ? -1 : 1;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    public static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        return Arrays.equals(Arrays.copyOf(key, prefix.length), prefix);
    }

    public static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
