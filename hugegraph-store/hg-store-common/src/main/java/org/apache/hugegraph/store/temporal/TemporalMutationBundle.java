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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Store-side temporal mutation bundle.
 *
 * The bundle is deliberately independent from GraphTransaction. It is the
 * payload whose final idempotency/conflict decision and four-view apply are
 * owned by the Store Raft apply path.
 */
public final class TemporalMutationBundle {

    public static final int CODEC_VERSION = 2;

    /** Wire operation codes; kept as explicit bytes so the wire is stable. */
    public enum Operation {

        APPEND((byte) 0),
        UPSERT((byte) 1),
        CLOSE((byte) 2),
        DELETE((byte) 3);

        private final byte code;

        Operation(byte code) {
            this.code = code;
        }

        public byte code() {
            return this.code;
        }

        public static Operation fromCode(int code) {
            for (Operation op : values()) {
                if (op.code() == (byte) code) {
                    return op;
                }
            }
            throw new IllegalArgumentException("unknown temporal operation code: " + code);
        }
    }

    private final Operation operation;
    private final String graph;
    private final String temporalLabel;
    private final String entityId;
    private final byte[] factKey;
    private final String mutationId;
    private final int schemaVersion;
    private final long validFrom;
    private final long validTo;
    private final boolean open;
    private final byte[] payload;
    private final List<ViewMutation> views;

    /**
     * Interval-creating bundle (APPEND/UPSERT). Kept for source compatibility:
     * the interval-creating operations carry exactly four views.
     */
    public TemporalMutationBundle(String graph, String temporalLabel, String entityId,
                                  byte[] factKey, String mutationId, int schemaVersion,
                                  long validFrom, long validTo, boolean open,
                                  byte[] payload, List<ViewMutation> views) {
        this(Operation.APPEND, graph, temporalLabel, entityId, factKey, mutationId,
             schemaVersion, validFrom, validTo, open, payload, views);
    }

    public TemporalMutationBundle(Operation operation, String graph, String temporalLabel,
                                  String entityId, byte[] factKey, String mutationId,
                                  int schemaVersion, long validFrom, long validTo,
                                  boolean open, byte[] payload, List<ViewMutation> views) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.graph = requireText(graph, "graph");
        this.temporalLabel = requireText(temporalLabel, "temporalLabel");
        this.entityId = requireText(entityId, "entityId");
        this.factKey = copy(factKey, "factKey");
        this.mutationId = requireText(mutationId, "mutationId");
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("schemaVersion must be positive");
        }
        if (validTo < validFrom && !open) {
            throw new IllegalArgumentException("validTo must be >= validFrom");
        }
        this.schemaVersion = schemaVersion;
        this.validFrom = validFrom;
        this.validTo = validTo;
        this.open = open;
        this.payload = copy(payload, "payload");
        this.views = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(views, "views")));
        boolean intervalCreating = operation == Operation.APPEND ||
                                   operation == Operation.UPSERT;
        if (intervalCreating && this.views.size() != 4) {
            throw new IllegalArgumentException(
                    "append/upsert bundle must contain four views");
        }
        if (!intervalCreating && this.views.size() != 0) {
            throw new IllegalArgumentException(
                    "close/delete bundle must contain zero views");
        }
    }

    public Operation operation() { return operation; }
    public String graph() { return graph; }
    public String temporalLabel() { return temporalLabel; }
    public String entityId() { return entityId; }
    public byte[] factKey() { return factKey.clone(); }
    public String mutationId() { return mutationId; }
    public int schemaVersion() { return schemaVersion; }
    public long validFrom() { return validFrom; }
    public long validTo() { return validTo; }
    public boolean open() { return open; }
    public byte[] payload() { return payload.clone(); }
    public List<ViewMutation> views() { return views; }

    private static String requireText(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static byte[] copy(byte[] value, String name) {
        if (value == null) {
            throw new NullPointerException(name);
        }
        return value.clone();
    }

    public static final class ViewMutation {
        private final String name;
        private final byte[] key;
        private final byte[] value;
        private final long committedRevision;

        public ViewMutation(String name, byte[] key, byte[] value) {
            this(name, key, value, -1L);
        }

        public ViewMutation(String name, byte[] key, byte[] value,
                            long committedRevision) {
            this.name = requireText(name, "view name");
            this.key = copy(key, "view key");
            this.value = copy(value, "view value");
            if (committedRevision == 0) {
                throw new IllegalArgumentException("committedRevision must be -1 or positive");
            }
            this.committedRevision = committedRevision;
        }

        public String name() { return name; }
        public byte[] key() { return key.clone(); }
        public byte[] value() { return value.clone(); }
        public long committedRevision() { return committedRevision; }
    }
}
