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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Business fact identity of one entity. It is NOT the entity id.
 *
 * The dimension order is fixed by schema. The client may not reorder or omit a
 * required dimension; the server recomputes the canonical bytes from schema
 * order and never trusts a client supplied encoding.
 */
public final class TemporalFactKey {

    /** Version prefix of the canonical fact key encoding. */
    public static final byte CANONICAL_VERSION = 1;

    private final List<String> dimensionOrder;
    private final Map<String, String> dimensions;
    private final byte[] canonicalBytes;

    private TemporalFactKey(List<String> order, Map<String, String> values) {
        this.dimensionOrder = Collections.unmodifiableList(order);
        this.dimensions = Collections.unmodifiableMap(values);
        this.canonicalBytes = encode(order, values);
    }

    /**
     * Build a fact key from the schema declared dimension order. The caller
     * passes the schema order explicitly; a missing dimension is rejected here
     * rather than silently dropped from the row key.
     */
    public static TemporalFactKey of(List<String> schemaDimensionOrder,
                                     Map<String, String> values) {
        if (schemaDimensionOrder == null || schemaDimensionOrder.isEmpty()) {
            throw new IllegalArgumentException(
                    "The temporal fact key schema order can't be empty");
        }
        LinkedHashMap<String, String> ordered = new LinkedHashMap<>();
        for (String dim : schemaDimensionOrder) {
            String value = values == null ? null : values.get(dim);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Missing required fact key dimension: " + dim);
            }
            ordered.put(dim, value);
        }
        if (values != null && values.size() != ordered.size()) {
            throw new IllegalArgumentException(
                    "Unexpected fact key dimensions, schema order is " +
                    schemaDimensionOrder + " but got " + values.keySet());
        }
        return new TemporalFactKey(schemaDimensionOrder, ordered);
    }

    private static byte[] encode(List<String> order, Map<String, String> values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(CANONICAL_VERSION);
        for (String dim : order) {
            writeLengthPrefixed(out, dim);
            writeLengthPrefixed(out, values.get(dim));
        }
        return out.toByteArray();
    }

    private static void writeLengthPrefixed(ByteArrayOutputStream out, String s) {
        byte[] raw = s.getBytes(StandardCharsets.UTF_8);
        out.write((raw.length >>> 24) & 0xFF);
        out.write((raw.length >>> 16) & 0xFF);
        out.write((raw.length >>> 8) & 0xFF);
        out.write(raw.length & 0xFF);
        out.write(raw, 0, raw.length);
    }

    /** Canonical bytes, always stored alongside the hash. */
    public byte[] canonicalBytes() {
        return Arrays.copyOf(this.canonicalBytes, this.canonicalBytes.length);
    }

    public List<String> dimensionOrder() {
        return this.dimensionOrder;
    }

    public Map<String, String> dimensions() {
        return this.dimensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TemporalFactKey)) {
            return false;
        }
        return Arrays.equals(this.canonicalBytes,
                             ((TemporalFactKey) o).canonicalBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.canonicalBytes);
    }

    @Override
    public String toString() {
        return "fact" + this.dimensions;
    }
}
