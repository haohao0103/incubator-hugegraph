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
import java.util.Arrays;
import java.util.Objects;

/**
 * Request and result types of one temporal mutation.
 *
 * valid_to == null means an open interval. Intervals are half-open:
 * [valid_from, valid_to).
 */
public final class TemporalWrite {

    private TemporalWrite() {
    }

    public enum Operation {
        APPEND,
        UPSERT,
        CLOSE,
        DELETE
    }

    public enum State {
        OPEN,
        CLOSED,
        TOMBSTONE
    }

    public enum Status {
        APPLIED,
        IDEMPOTENT_NOOP
    }

    /** Graph-element kind a temporal write may be bound to (Phase C). */
    public enum ElementKind {
        VERTEX,
        EDGE
    }

    /**
     * Immutable binding of a temporal write to one graph element (Phase C).
     *
     * The binding is additive metadata: it never enters {@code VertexId} /
     * {@code EdgeId}, never changes id generation or sortKeys, and never alters
     * the fact key / entity id / interval / idempotency fingerprint. It only
     * tells the Store to also maintain the {@code g+temporal_element_index}
     * entry (element_id -> valid intervals) in the same atomic transaction.
     */
    public static final class ElementBinding {

        private final ElementKind kind;
        private final String elementId;
        private final String elementLabel;

        public ElementBinding(ElementKind kind, String elementId, String elementLabel) {
            if (kind == null) {
                throw new IllegalArgumentException("element kind can't be null");
            }
            if (elementId == null || elementId.isEmpty()) {
                throw new IllegalArgumentException("element id can't be null or empty");
            }
            if (elementLabel == null || elementLabel.isEmpty()) {
                throw new IllegalArgumentException("element label can't be null or empty");
            }
            this.kind = kind;
            this.elementId = elementId;
            this.elementLabel = elementLabel;
        }

        public ElementKind kind() {
            return this.kind;
        }

        public String elementId() {
            return this.elementId;
        }

        public String elementLabel() {
            return this.elementLabel;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ElementBinding)) {
                return false;
            }
            ElementBinding other = (ElementBinding) o;
            return this.kind == other.kind &&
                   this.elementId.equals(other.elementId) &&
                   this.elementLabel.equals(other.elementLabel);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.kind, this.elementId, this.elementLabel);
        }

        @Override
        public String toString() {
            return "ElementBinding{" + this.kind + " " + this.elementLabel +
                   ":" + this.elementId + "}";
        }
    }

    public static final class Request {

        private final Operation operation;
        private final String graphId;
        private final String temporalLabel;
        private final String entityId;
        private final TemporalFactKey factKey;
        private final long validFrom;
        private final Long validTo;
        private final byte[] payload;
        private final String mutationId;
        private final int schemaVersion;
        private final ElementBinding elementBinding;

        public Request(Operation operation, String graphId, String temporalLabel,
                       String entityId, TemporalFactKey factKey,
                       long validFrom, Long validTo, byte[] payload,
                       String mutationId, int schemaVersion) {
            this(operation, graphId, temporalLabel, entityId, factKey, validFrom, validTo,
                 payload, mutationId, schemaVersion, null);
        }

        /**
         * Full constructor (Phase C): additionally carries an optional
         * graph-element binding. {@code null} means unbound (current-only), the
         * pre-Phase-C behavior. The binding is additive and never relaxes the
         * half-open interval validation below.
         */
        public Request(Operation operation, String graphId, String temporalLabel,
                       String entityId, TemporalFactKey factKey,
                       long validFrom, Long validTo, byte[] payload,
                       String mutationId, int schemaVersion,
                       ElementBinding elementBinding) {
            if (validTo != null && validTo <= validFrom) {
                throw new IllegalArgumentException(
                        "Half-open interval requires valid_to > valid_from");
            }
            this.operation = operation;
            this.graphId = graphId;
            this.temporalLabel = temporalLabel;
            this.entityId = entityId;
            this.factKey = factKey;
            this.validFrom = validFrom;
            this.validTo = validTo;
            this.payload = payload == null ? new byte[0]
                                           : Arrays.copyOf(payload, payload.length);
            this.mutationId = mutationId;
            this.schemaVersion = schemaVersion;
            this.elementBinding = elementBinding;
        }

        public static Request append(String graph, String label, String entity,
                                     TemporalFactKey factKey, long from, Long to,
                                     String payload, String mutationId) {
            return new Request(Operation.APPEND, graph, label, entity, factKey,
                               from, to, bytes(payload), mutationId, 1);
        }

        public static Request upsert(String graph, String label, String entity,
                                     TemporalFactKey factKey, long from, Long to,
                                     String payload, String mutationId) {
            return new Request(Operation.UPSERT, graph, label, entity, factKey,
                               from, to, bytes(payload), mutationId, 1);
        }

        public static Request close(String graph, String label, String entity,
                                    TemporalFactKey factKey, long from,
                                    long closeAt, String mutationId) {
            return new Request(Operation.CLOSE, graph, label, entity, factKey,
                               from, closeAt, new byte[0], mutationId, 1);
        }

        public static Request delete(String graph, String label, String entity,
                                     TemporalFactKey factKey, long from,
                                     String mutationId) {
            return new Request(Operation.DELETE, graph, label, entity, factKey,
                               from, null, new byte[0], mutationId, 1);
        }

        private static byte[] bytes(String s) {
            return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
        }

        public Operation operation() {
            return this.operation;
        }

        public String graphId() {
            return this.graphId;
        }

        public String temporalLabel() {
            return this.temporalLabel;
        }

        public String entityId() {
            return this.entityId;
        }

        public TemporalFactKey factKey() {
            return this.factKey;
        }

        public long validFrom() {
            return this.validFrom;
        }

        public Long validTo() {
            return this.validTo;
        }

        public byte[] payload() {
            return Arrays.copyOf(this.payload, this.payload.length);
        }

        public String mutationId() {
            return this.mutationId;
        }

        public int schemaVersion() {
            return this.schemaVersion;
        }

        /** Optional graph-element binding (Phase C); {@code null} when unbound. */
        public ElementBinding elementBinding() {
            return this.elementBinding;
        }

        /**
         * Return a copy bound to a graph element (Phase C). Purely additive: the
         * fact key, entity id, interval and idempotency fingerprint are unchanged,
         * so a bound and an unbound request with the same fields apply identically
         * on the interval views; the binding only drives the extra element-index
         * write on the Store apply path.
         */
        public Request withElement(ElementKind kind, String elementId, String elementLabel) {
            return new Request(this.operation, this.graphId, this.temporalLabel,
                               this.entityId, this.factKey, this.validFrom, this.validTo,
                               this.payload, this.mutationId, this.schemaVersion,
                               new ElementBinding(kind, elementId, elementLabel));
        }

        public ColocationGroup group() {
            return new ColocationGroup(this.graphId, this.temporalLabel,
                                       this.entityId, this.factKey);
        }

        /**
         * Canonical interval payload used both as the tie_breaker input and as
         * the idempotency fingerprint source.
         */
        public byte[] canonicalIntervalPayload() {
            return TieBreakers.concat(
                    this.operation.name().getBytes(StandardCharsets.UTF_8),
                    longBytes(this.validFrom),
                    this.validTo == null ? new byte[0] : longBytes(this.validTo),
                    this.payload,
                    longBytes(this.schemaVersion));
        }

        private static byte[] longBytes(long v) {
            byte[] out = new byte[8];
            for (int i = 7; i >= 0; i--) {
                out[7 - i] = (byte) ((v >>> (i * 8)) & 0xFF);
            }
            return out;
        }
    }

    public static final class Result {

        private final Status status;
        private final long revision;
        private final byte[] historyRowKey;
        private final String tieBreaker;
        private final long currentRevision;
        private final long openIndexRevision;

        public Result(Status status, long revision, byte[] historyRowKey,
                      String tieBreaker, long currentRevision,
                      long openIndexRevision) {
            this.status = status;
            this.revision = revision;
            this.historyRowKey = historyRowKey;
            this.tieBreaker = tieBreaker;
            this.currentRevision = currentRevision;
            this.openIndexRevision = openIndexRevision;
        }

        public Status status() {
            return this.status;
        }

        public long revision() {
            return this.revision;
        }

        public byte[] historyRowKey() {
            return this.historyRowKey;
        }

        public String tieBreaker() {
            return this.tieBreaker;
        }

        public long currentRevision() {
            return this.currentRevision;
        }

        public long openIndexRevision() {
            return this.openIndexRevision;
        }
    }

    /** Immutable history record. */
    public static final class HistoryRow {

        final byte[] rowKey;
        final TemporalFactKey factKey;
        final long validFrom;
        Long validTo;
        State state;
        final String mutationId;
        final String tieBreaker;
        final byte[] payload;
        long revision;

        HistoryRow(byte[] rowKey, TemporalFactKey factKey, long validFrom,
                   Long validTo, State state, String mutationId,
                   String tieBreaker, byte[] payload, long revision) {
            this.rowKey = rowKey;
            this.factKey = factKey;
            this.validFrom = validFrom;
            this.validTo = validTo;
            this.state = state;
            this.mutationId = mutationId;
            this.tieBreaker = tieBreaker;
            this.payload = payload;
            this.revision = revision;
        }

        public byte[] rowKey() {
            return this.rowKey;
        }

        public TemporalFactKey factKey() {
            return this.factKey;
        }

        public long validFrom() {
            return this.validFrom;
        }

        public Long validTo() {
            return this.validTo;
        }

        public State state() {
            return this.state;
        }

        public String tieBreaker() {
            return this.tieBreaker;
        }

        public long revision() {
            return this.revision;
        }

        public String payloadAsString() {
            return new String(this.payload, StandardCharsets.UTF_8);
        }
    }

    /** Materialized current view pointer, not a recomputed aggregate. */
    public static final class CurrentPointer {

        final TemporalFactKey factKey;
        long validFrom;
        Long validTo;
        State state;
        long revision;

        CurrentPointer(TemporalFactKey factKey, long validFrom, Long validTo,
                       State state, long revision) {
            this.factKey = factKey;
            this.validFrom = validFrom;
            this.validTo = validTo;
            this.state = state;
            this.revision = revision;
        }

        public long validFrom() {
            return this.validFrom;
        }

        public Long validTo() {
            return this.validTo;
        }

        public State state() {
            return this.state;
        }

        public long revision() {
            return this.revision;
        }
    }

    /** Cross-bucket open interval index entry. */
    public static final class OpenIntervalEntry {

        final byte[] indexKey;
        final TemporalFactKey factKey;
        final long validFrom;
        final byte[] rowKey;
        long revision;

        OpenIntervalEntry(byte[] indexKey, TemporalFactKey factKey,
                          long validFrom, byte[] rowKey, long revision) {
            this.indexKey = indexKey;
            this.factKey = factKey;
            this.validFrom = validFrom;
            this.rowKey = rowKey;
            this.revision = revision;
        }

        public long validFrom() {
            return this.validFrom;
        }

        public long revision() {
            return this.revision;
        }
    }
}
