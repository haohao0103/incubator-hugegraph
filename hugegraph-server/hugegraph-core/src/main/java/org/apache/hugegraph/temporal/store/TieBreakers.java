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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * tie_breaker derivation frozen by the design ruling section 2.1:
 *
 * tie_breaker = base32(SHA-256(concat(mutation_id,
 *                                     canonical_fact_key,
 *                                     canonical_interval_payload)))[0:20]
 *
 * concat is defined as unambiguous length-prefixed concatenation, so that two
 * different component splits can never produce the same digest input.
 *
 * The value is derived from the client supplied mutation_id only. It is not
 * random and not derived from Store apply order, therefore replay, leader
 * switch and restart do not change the ordering key.
 */
public final class TieBreakers {

    public static final int LENGTH = 20;

    private static final char[] BASE32 =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".toCharArray();

    /**
     * Pooled SHA-256 digest, one instance per thread ({@link MessageDigest} is
     * not thread safe). The previous per-call {@code MessageDigest.getInstance}
     * paid a JCA provider lookup on every write request; the digest is used for
     * the tie_breaker, the fact-key hash and the colocation hash.
     */
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required", e);
        }
    });

    private TieBreakers() {
    }

    public static String derive(String mutationId,
                                byte[] canonicalFactKey,
                                byte[] canonicalIntervalPayload) {
        if (mutationId == null || mutationId.isEmpty()) {
            throw new IllegalArgumentException(
                    "The mutation_id can't be null or empty; the server never " +
                    "generates a substitute id");
        }
        byte[] input = concat(mutationId.getBytes(StandardCharsets.UTF_8),
                              canonicalFactKey,
                              canonicalIntervalPayload);
        byte[] digest = sha256(input);
        return base32(digest).substring(0, LENGTH);
    }

    /**
     * Unambiguous length-prefixed concatenation. The result is pre-sized in
     * one pass instead of growing a {@code ByteArrayOutputStream}; the emitted
     * bytes are identical (4-byte big-endian length, then the payload).
     */
    static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] part : parts) {
            total += Integer.BYTES + (part == null ? 0 : part.length);
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] part : parts) {
            int len = part == null ? 0 : part.length;
            out[pos++] = (byte) ((len >>> 24) & 0xFF);
            out[pos++] = (byte) ((len >>> 16) & 0xFF);
            out[pos++] = (byte) ((len >>> 8) & 0xFF);
            out[pos++] = (byte) (len & 0xFF);
            if (len > 0) {
                System.arraycopy(part, 0, out, pos, len);
                pos += len;
            }
        }
        return out;
    }

    static byte[] sha256(byte[] input) {
        MessageDigest digest = SHA256.get();
        digest.reset();
        return digest.digest(input);
    }

    static String base32(byte[] data) {
        StringBuilder sb = new StringBuilder();
        int buffer = 0;
        int bitsLeft = 0;
        for (byte b : data) {
            buffer = (buffer << 8) | (b & 0xFF);
            bitsLeft += 8;
            while (bitsLeft >= 5) {
                sb.append(BASE32[(buffer >>> (bitsLeft - 5)) & 0x1F]);
                bitsLeft -= 5;
            }
        }
        if (bitsLeft > 0) {
            sb.append(BASE32[(buffer << (5 - bitsLeft)) & 0x1F]);
        }
        return sb.toString();
    }
}
