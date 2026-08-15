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
 * Single source of truth for the Store-side temporal interval marker layout,
 * shared by the write path ({@link TemporalMutationHandler}) and the read path
 * ({@link TemporalQueryHandler}).
 *
 * <p>Phase 4 layout: the marker key is bucketed so a fact-scoped read can seek
 * straight to the bucket containing a query time instead of scanning the whole
 * fact sequence (design ruling §4):</p>
 *
 * <pre>
 *   key   = fact_key || marker_version || sortable(time_bucket) || sortable(valid_from) || sortable(valid_to)
 *   value = big-endian(committed_revision) || state || payload
 * </pre>
 *
 * <p>{@code sortable(long) = long ^ Long.MIN_VALUE} (sign-flipped big endian) so
 * the lexicographic byte order equals the numeric order, including negative
 * epoch millis. {@code time_bucket = floorDiv(valid_from, BUCKET_WIDTH_MILLIS)};
 * an interval lives in its {@code valid_from} bucket. An open interval stores
 * {@code valid_to = OPEN_VALID_TO} (Long.MAX_VALUE). The marker key length is
 * exactly {@code fact_key.length + MARKER_BYTES} (version byte + 3 longs); any
 * key under the same {@code fact_key} prefix with a different length or version
 * belongs to a different fact key (a fact-key hash collision) or a different
 * marker layout and must be filtered out, never treated as part of this fact
 * sequence.</p>
 */
public final class TemporalIntervalCodec {

    public static final int BUCKET_BYTES = Long.BYTES;

    /**
     * Marker layout version byte, written right after the fact_key (design
     * ruling §2.1: "row-key encoding version is recorded as a fixed prefix").
     * A reader that does not recognize the version must refuse, never guess.
     */
    public static final byte MARKER_VERSION = 1;

    /** Number of suffix bytes after the fact_key: version || bucket || from || to. */
    public static final int MARKER_BYTES = 1 + Long.BYTES * 3;

    /** Sentinel {@code valid_to} for an open (still valid) interval. */
    public static final long OPEN_VALID_TO = Long.MAX_VALUE;

    /**
     * Sentinel bucket for open markers: {@code sortable(OPEN_BUCKET)} sorts
     * before every real time-bucket, so the open markers of a fact sequence form
     * a tiny leading region that a read can locate in one seek instead of
     * walking back across every bucket (design ruling §4 open-index walk-back).
     */
    public static final long OPEN_BUCKET = Long.MIN_VALUE;

    /** Marker states carried in the marker value. */
    public static final byte STATE_ACTIVE = 0;
    public static final byte STATE_TOMBSTONE = 1;

    private static final int STATE_OFFSET = Long.BYTES;

    /** Default valid-time bucket width: 7 days (design section 2.1). */
    public static final long BUCKET_WIDTH_MILLIS = 7L * 24 * 3600 * 1000;

    private TemporalIntervalCodec() {
    }

    /** Bucket index derived from {@code validFromMillis}, floor division. */
    public static long bucketOf(long validFromMillis) {
        return Math.floorDiv(validFromMillis, BUCKET_WIDTH_MILLIS);
    }

    public static byte[] intervalKey(byte[] factKey, long validFrom, long validTo) {
        long bucket = validTo == OPEN_VALID_TO ? OPEN_BUCKET : bucketOf(validFrom);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(factKey, 0, factKey.length);
        out.write(MARKER_VERSION);
        writeSortableLong(out, bucket);
        writeSortableLong(out, validFrom);
        writeSortableLong(out, validTo);
        return out.toByteArray();
    }

    public static byte[] intervalKey(TemporalMutationBundle bundle) {
        return intervalKey(bundle.factKey(), bundle.validFrom(),
                           bundle.open() ? OPEN_VALID_TO : bundle.validTo());
    }

    /** Prefix of every marker of one fact sequence that lives in {@code bucket}. */
    public static byte[] bucketPrefix(byte[] factKey, long bucket) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(factKey, 0, factKey.length);
        out.write(MARKER_VERSION);
        writeSortableLong(out, bucket);
        return out.toByteArray();
    }

    /**
     * Revision-prefixed active value: {@code revision || ACTIVE || payload}.
     * Shared by the four views, the ledger and the active interval marker.
     */
    public static byte[] value(byte[] payload, long revision) {
        return value(payload, revision, STATE_ACTIVE);
    }

    /** State-carrying marker value: {@code revision || state || payload}. */
    public static byte[] value(byte[] payload, long revision, byte state) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + payload.length);
        buffer.putLong(revision).put(state).put(payload);
        return buffer.array();
    }

    /**
     * Parse an interval marker key that exactly matches {@code factKey}.
     *
     * @return the parsed interval, or {@code null} when the key belongs to a
     *         different fact key sharing the same prefix (hash collision) or is
     *         a non-marker row; the caller must skip {@code null} results.
     */
    public static Interval parse(byte[] key, byte[] factKey) {
        if (key.length != factKey.length + MARKER_BYTES) {
            return null;
        }
        for (int i = 0; i < factKey.length; i++) {
            if (key[i] != factKey[i]) {
                return null;
            }
        }
        if (key[factKey.length] != MARKER_VERSION) {
            // A different (future or legacy) marker layout must be refused, not
            // misparsed as this fact sequence.
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(key, factKey.length + 1, MARKER_BYTES - 1);
        long bucket = readSortableLong(buffer);
        long from = readSortableLong(buffer);
        long to = readSortableLong(buffer);
        boolean open = to == OPEN_VALID_TO;
        return new Interval(bucket, from, open ? null : to, open);
    }

    /**
     * Committed revision prefix of an interval value, or {@code -1} when the
     * value is absent or shorter than a revision.
     */
    public static long revision(byte[] value) {
        if (value == null || value.length < Long.BYTES) {
            return -1L;
        }
        return ByteBuffer.wrap(value, 0, Long.BYTES).getLong();
    }

    /**
     * Marker state of a marker value. A value shorter than revision+state (a
     * pre-state-byte layout) is treated as ACTIVE.
     */
    public static byte state(byte[] value) {
        if (value == null || value.length <= STATE_OFFSET) {
            return STATE_ACTIVE;
        }
        return value[STATE_OFFSET];
    }

    /** Payload of a marker value (after the revision + state prefix). */
    public static byte[] payload(byte[] value) {
        if (value == null || value.length <= STATE_OFFSET + 1) {
            return new byte[0];
        }
        int length = value.length - (STATE_OFFSET + 1);
        byte[] payload = new byte[length];
        System.arraycopy(value, STATE_OFFSET + 1, payload, 0, length);
        return payload;
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

    /** Decoded interval marker. */
    public static final class Interval {

        public final long bucket;
        public final long validFrom;
        /** {@code null} means open (still valid). */
        public final Long validTo;
        public final boolean open;

        Interval(long bucket, long validFrom, Long validTo, boolean open) {
            this.bucket = bucket;
            this.validFrom = validFrom;
            this.validTo = validTo;
            this.open = open;
        }
    }
}
