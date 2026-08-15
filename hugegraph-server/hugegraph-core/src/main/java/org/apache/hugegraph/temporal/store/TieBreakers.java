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

    /** Unambiguous length-prefixed concatenation. */
    static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            byte[] p = part == null ? new byte[0] : part;
            out.write((p.length >>> 24) & 0xFF);
            out.write((p.length >>> 16) & 0xFF);
            out.write((p.length >>> 8) & 0xFF);
            out.write(p.length & 0xFF);
            out.write(p, 0, p.length);
        }
        return out.toByteArray();
    }

    static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required", e);
        }
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
