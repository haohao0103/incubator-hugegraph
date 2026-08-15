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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hugegraph.temporal.store.TemporalWrite.CurrentPointer;
import org.apache.hugegraph.temporal.store.TemporalWrite.HistoryRow;
import org.apache.hugegraph.temporal.store.TemporalWrite.OpenIntervalEntry;
import org.apache.hugegraph.temporal.store.TemporalWrite.Operation;
import org.apache.hugegraph.temporal.store.TemporalWrite.Request;
import org.apache.hugegraph.temporal.store.TemporalWrite.Result;
import org.apache.hugegraph.temporal.store.TemporalWrite.State;
import org.apache.hugegraph.temporal.store.TemporalWrite.Status;

/**
 * Invariant-level skeleton of TemporalBackendStore.
 *
 * SCOPE WARNING - this class is NOT a backend and NOT an acceptance target.
 * It holds state in memory only so that the cluster independent invariants of
 * the frozen design ruling can be unit tested before HStore integration:
 * row key encoding and ordering, mutation_id derived tie breaker, raw fact key
 * identity under hash collision, idempotency, half-open conflict detection,
 * cross-bucket open interval index, closed interval immutability, colocation
 * placement and capacity rejection.
 *
 * It produces no throughput, latency, conflict-rate or fault-recovery
 * conclusion. Those may only be measured on a real HStore / PD / Raft cluster.
 * RocksDB, Memory and HBase are explicitly excluded as temporal acceptance
 * substitutes.
 */
public class TemporalBackendStoreSkeleton {

    private static final Comparator<byte[]> BYTES =
            TemporalRowKeyCodec::compare;

    private final TemporalRowKeyCodec codec;
    private final ColocationPlacement placement;
    private final long maxRowsPerGroup;
    private final long maxBytesPerGroup;
    private final int supportedSchemaVersion;

    /**
     * Key space is indexed by the row key prefix, exactly like a real Store
     * seek. The prefix only carries fact_key_hash, so every read must still
     * filter on the raw canonical fact key.
     */
    private final Map<ByteKey, PrefixSpace> keySpace = new LinkedHashMap<>();
    private final Map<ColocationGroup, GroupMeta> groups = new LinkedHashMap<>();
    private final Map<String, CommittedMutation> idempotency = new HashMap<>();

    private long revision = 0L;
    private boolean openIntervalIndexEnabled = true;

    public TemporalBackendStoreSkeleton(TemporalRowKeyCodec codec,
                                        ColocationPlacement placement,
                                        long maxRowsPerGroup,
                                        long maxBytesPerGroup) {
        this.codec = codec;
        this.placement = placement;
        this.maxRowsPerGroup = maxRowsPerGroup;
        this.maxBytesPerGroup = maxBytesPerGroup;
        this.supportedSchemaVersion = 1;
    }

    /**
     * Negative control switch used by the cross-bucket test to prove the open
     * interval index is load bearing. Production has no such switch.
     */
    public void setOpenIntervalIndexEnabled(boolean enabled) {
        this.openIntervalIndexEnabled = enabled;
    }

    public TemporalRowKeyCodec codec() {
        return this.codec;
    }

    public long revision() {
        return this.revision;
    }

    // ---------------------------------------------------------------- writes

    public Result write(Request req) {
        checkCapability(req);

        CommittedMutation prior = this.idempotency.get(req.mutationId());
        if (prior != null) {
            if (Arrays.equals(prior.fingerprint, fingerprintOf(req))) {
                return new Result(Status.IDEMPOTENT_NOOP, prior.revision,
                                  prior.historyRowKey, prior.tieBreaker,
                                  prior.revision, prior.revision);
            }
            Map<String, Object> diag = new LinkedHashMap<>();
            diag.put("mutation_id", req.mutationId());
            diag.put("committed_revision", prior.revision);
            throw new TemporalStoreException(
                    TemporalErrorCode.IDEMPOTENCY_CONFLICT,
                    "mutation_id replayed with a different canonical fact key, " +
                    "interval or payload", diag);
        }

        ColocationGroup group = req.group();
        checkPlacement(group);
        checkCapacity(group, req);

        switch (req.operation()) {
            case APPEND:
                return this.doAppend(req, group);
            case UPSERT:
                return this.doUpsert(req, group);
            case CLOSE:
                return this.doClose(req, group);
            case DELETE:
                return this.doDelete(req, group);
            default:
                throw new IllegalArgumentException(
                        "Unsupported operation: " + req.operation());
        }
    }

    private Result doAppend(Request req, ColocationGroup group) {
        checkOverlap(req, group);
        return commit(req, group, req.validTo());
    }

    private Result doUpsert(Request req, ColocationGroup group) {
        HistoryRow sameStart = findRow(group, req.validFrom());
        if (sameStart != null && sameStart.state() == State.CLOSED) {
            Map<String, Object> diag = new LinkedHashMap<>();
            diag.put("colocation_group", group.toString());
            diag.put("valid_from", req.validFrom());
            diag.put("valid_to", sameStart.validTo());
            diag.put("state", sameStart.state().name());
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_CLOSED_INTERVAL_CONFLICT,
                    "a closed interval can't be modified or reopened in place; " +
                    "historical correction must use a separate flow", diag);
        }
        if (sameStart != null && sameStart.state() == State.OPEN) {
            if (!Arrays.equals(sameStart.payload, req.payload())) {
                Map<String, Object> diag = new LinkedHashMap<>();
                diag.put("colocation_group", group.toString());
                diag.put("valid_from", req.validFrom());
                throw new TemporalStoreException(
                        TemporalErrorCode.TEMPORAL_CONFLICT,
                        "same valid-time with a different payload", diag);
            }
            // Same valid-time written by a different mutation_id is an
            // interval intersection, not an idempotency replay. Only the
            // original mutation_id may retry.
            Map<String, Object> diag = new LinkedHashMap<>();
            diag.put("colocation_group", group.toString());
            diag.put("owner_mutation_id", sameStart.mutationId);
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_CONFLICT,
                    "interval already owned by another mutation_id; retry must " +
                    "reuse the original mutation_id", diag);
        }
        checkOverlapForUpsert(req, group);
        return commit(req, group, req.validTo());
    }

    private Result doClose(Request req, ColocationGroup group) {
        HistoryRow row = findRow(group, req.validFrom());
        if (row == null) {
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_CONFLICT,
                    "no interval to close at valid_from=" + req.validFrom());
        }
        if (row.state() == State.CLOSED) {
            if (row.validTo() != null && row.validTo().equals(req.validTo())) {
                return new Result(Status.IDEMPOTENT_NOOP, row.revision(),
                                  row.rowKey(), row.tieBreaker(),
                                  row.revision(), row.revision());
            }
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_CLOSED_INTERVAL_CONFLICT,
                    "interval already closed at " + row.validTo());
        }
        long rev = ++this.revision;
        row.validTo = req.validTo();
        row.state = State.CLOSED;
        row.revision = rev;

        GroupMeta meta = meta(group);
        if (meta.current != null && meta.current.validFrom() == req.validFrom()) {
            meta.current.validTo = req.validTo();
            meta.current.state = State.CLOSED;
            meta.current.revision = rev;
        }

        PrefixSpace space = space(group);
        space.openIndex.entrySet().removeIf(
                e -> e.getValue().factKey.equals(group.factKey()) &&
                     e.getValue().validFrom == req.validFrom());

        this.idempotency.put(req.mutationId(),
                             new CommittedMutation(fingerprintOf(req), rev,
                                                   row.rowKey(),
                                                   row.tieBreaker()));
        return new Result(Status.APPLIED, rev, row.rowKey(), row.tieBreaker(),
                          rev, rev);
    }

    private Result doDelete(Request req, ColocationGroup group) {
        HistoryRow row = findRow(group, req.validFrom());
        if (row == null) {
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_CONFLICT,
                    "no interval to delete at valid_from=" + req.validFrom());
        }
        long rev = ++this.revision;
        row.state = State.TOMBSTONE;
        row.revision = rev;
        GroupMeta meta = meta(group);
        meta.current.state = State.TOMBSTONE;
        meta.current.revision = rev;
        space(group).openIndex.entrySet().removeIf(
                e -> e.getValue().factKey.equals(group.factKey()) &&
                     e.getValue().validFrom == req.validFrom());
        this.idempotency.put(req.mutationId(),
                             new CommittedMutation(fingerprintOf(req), rev,
                                                   row.rowKey(),
                                                   row.tieBreaker()));
        return new Result(Status.APPLIED, rev, row.rowKey(), row.tieBreaker(),
                          rev, rev);
    }

    /**
     * Single revision apply of history row, current pointer and open interval
     * index. There is no asynchronous back-fill of any view.
     */
    private Result commit(Request req, ColocationGroup group, Long validTo) {
        State state = validTo == null ? State.OPEN : State.CLOSED;
        String tieBreaker = TieBreakers.derive(
                req.mutationId(),
                group.factKey().canonicalBytes(),
                req.canonicalIntervalPayload());
        byte[] rowKey = this.codec.historyRowKey(
                group.graphId(), group.temporalLabel(), group.entityId(),
                group.factKey(), req.validFrom(), tieBreaker);

        long rev = ++this.revision;
        PrefixSpace space = space(group);
        long bucket = this.codec.bucketOf(req.validFrom());

        HistoryRow row = new HistoryRow(rowKey, group.factKey(),
                                        req.validFrom(), validTo, state,
                                        req.mutationId(), tieBreaker,
                                        req.payload(), rev);
        space.bucket(bucket).put(rowKey, row);

        GroupMeta meta = meta(group);
        meta.current = new CurrentPointer(group.factKey(), req.validFrom(),
                                          validTo, state, rev);
        meta.rows += 1;
        meta.bytes += req.payload().length + rowKey.length;

        long openIndexRevision = rev;
        if (validTo == null) {
            byte[] indexKey = TieBreakers.concat(
                    this.codec.groupPrefix(group.graphId(),
                                           group.temporalLabel(),
                                           group.entityId(),
                                           group.factKey()),
                    new byte[]{(byte) 0x4F},
                    longBytes(req.validFrom()),
                    tieBreaker.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            space.openIndex.put(indexKey,
                                new OpenIntervalEntry(indexKey, group.factKey(),
                                                      req.validFrom(), rowKey,
                                                      rev));
        }

        this.idempotency.put(req.mutationId(),
                             new CommittedMutation(fingerprintOf(req), rev,
                                                   rowKey, tieBreaker));
        return new Result(Status.APPLIED, rev, rowKey, tieBreaker,
                          rev, openIndexRevision);
    }

    // ------------------------------------------------------------- pre-check

    private void checkCapability(Request req) {
        if (req.schemaVersion() != this.supportedSchemaVersion) {
            Map<String, Object> diag = new LinkedHashMap<>();
            diag.put("requested_schema_version", req.schemaVersion());
            diag.put("supported_schema_version", this.supportedSchemaVersion);
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_UNSUPPORTED_VERSION,
                    "unsupported temporal schema version", diag);
        }
    }

    private void checkPlacement(ColocationGroup group) {
        int base = this.placement.regionOf(group, ColocationPlacement.View.HISTORY);
        for (ColocationPlacement.View view : this.placement.views()) {
            int region = this.placement.regionOf(group, view);
            if (region != base) {
                Map<String, Object> diag = new LinkedHashMap<>();
                diag.put("colocation_group", group.toString());
                diag.put("history_region", base);
                diag.put("view", view.name());
                diag.put("view_region", region);
                throw new TemporalStoreException(
                        TemporalErrorCode.TEMPORAL_CROSS_REGION_UNSUPPORTED,
                        "one temporal mutation touches multiple Regions; " +
                        "partial commit and async cross-Region back-fill are " +
                        "not allowed", diag);
            }
        }
    }

    private void checkCapacity(ColocationGroup group, Request req) {
        GroupMeta meta = meta(group);
        long nextRows = meta.rows + 1;
        long nextBytes = meta.bytes + req.payload().length;
        if (nextRows > this.maxRowsPerGroup || nextBytes > this.maxBytesPerGroup) {
            Map<String, Object> diag = new LinkedHashMap<>();
            diag.put("colocation_group", group.toString());
            diag.put("region_id", this.placement.baseRegionOf(group));
            diag.put("current_rows", meta.rows);
            diag.put("current_bytes", meta.bytes);
            diag.put("max_rows", this.maxRowsPerGroup);
            diag.put("max_bytes", this.maxBytesPerGroup);
            throw new TemporalStoreException(
                    TemporalErrorCode.TEMPORAL_COLOCATION_CAPACITY_EXCEEDED,
                    "colocation group reached the single Region limit; the " +
                    "fact sequence is never split across Regions and never " +
                    "downgraded to another backend", diag);
        }
    }

    /**
     * Half-open overlap detection. Candidates come from the target bucket only,
     * exactly like a bucket scoped Store seek, plus the cross-bucket open
     * interval index. Raw fact key equality is always re-checked because the
     * row key prefix only carries fact_key_hash.
     */
    private void checkOverlap(Request req, ColocationGroup group) {
        long newFrom = req.validFrom();
        long newTo = req.validTo() == null ? Long.MAX_VALUE : req.validTo();
        for (HistoryRow row : overlapCandidates(group, newFrom, newTo)) {
            long oldFrom = row.validFrom();
            long oldTo = row.validTo() == null ? Long.MAX_VALUE : row.validTo();
            if (oldFrom < newTo && oldTo > newFrom) {
                Map<String, Object> diag = new LinkedHashMap<>();
                diag.put("colocation_group", group.toString());
                diag.put("existing_interval", "[" + oldFrom + ", " +
                                              (row.validTo() == null ? "open" :
                                               row.validTo()) + ")");
                diag.put("new_interval", "[" + newFrom + ", " +
                                         (req.validTo() == null ? "open" :
                                          req.validTo()) + ")");
                throw new TemporalStoreException(
                        TemporalErrorCode.TEMPORAL_CONFLICT,
                        "half-open interval intersection on the same fact key",
                        diag);
            }
        }
    }

    private void checkOverlapForUpsert(Request req, ColocationGroup group) {
        this.checkOverlap(req, group);
    }

    private List<HistoryRow> overlapCandidates(ColocationGroup group,
                                               long newFrom, long newTo) {
        List<HistoryRow> candidates = new ArrayList<>();
        PrefixSpace space = space(group);
        long bucket = this.codec.bucketOf(newFrom);

        NavigableMap<byte[], HistoryRow> target = space.buckets.get(bucket);
        if (target != null) {
            for (HistoryRow row : target.values()) {
                if (!row.factKey().equals(group.factKey())) {
                    // fact_key_hash collision: same prefix, different fact
                    continue;
                }
                if (row.state() == State.TOMBSTONE) {
                    continue;
                }
                if (row.validFrom() >= newTo) {
                    continue;
                }
                candidates.add(row);
            }
        }
        if (this.openIntervalIndexEnabled) {
            for (OpenIntervalEntry entry : space.openIndex.values()) {
                if (!entry.factKey.equals(group.factKey())) {
                    continue;
                }
                HistoryRow row = space.findByRowKey(entry.rowKey);
                if (row != null && !candidates.contains(row) &&
                    row.state() != State.TOMBSTONE) {
                    candidates.add(row);
                }
            }
        }
        return candidates;
    }

    // ---------------------------------------------------------------- reads

    /** Fact-scoped history read, ordered by the Store row key order. */
    public List<HistoryRow> historyOf(ColocationGroup group) {
        List<HistoryRow> rows = new ArrayList<>();
        PrefixSpace space = this.keySpace.get(prefixKey(group));
        if (space == null) {
            return rows;
        }
        for (NavigableMap<byte[], HistoryRow> bucket : space.buckets.values()) {
            for (HistoryRow row : bucket.values()) {
                if (row.factKey().equals(group.factKey())) {
                    rows.add(row);
                }
            }
        }
        rows.sort((a, b) -> TemporalRowKeyCodec.compare(a.rowKey(), b.rowKey()));
        return rows;
    }

    public CurrentPointer currentOf(ColocationGroup group) {
        GroupMeta meta = this.groups.get(group);
        return meta == null ? null : meta.current;
    }

    public List<OpenIntervalEntry> openIntervalsOf(ColocationGroup group) {
        List<OpenIntervalEntry> entries = new ArrayList<>();
        PrefixSpace space = this.keySpace.get(prefixKey(group));
        if (space == null) {
            return entries;
        }
        for (OpenIntervalEntry entry : space.openIndex.values()) {
            if (entry.factKey.equals(group.factKey())) {
                entries.add(entry);
            }
        }
        return entries;
    }

    /** Number of distinct fact keys sharing one row key prefix. */
    public int factKeysUnderPrefix(ColocationGroup group) {
        PrefixSpace space = this.keySpace.get(prefixKey(group));
        if (space == null) {
            return 0;
        }
        List<TemporalFactKey> seen = new ArrayList<>();
        for (NavigableMap<byte[], HistoryRow> bucket : space.buckets.values()) {
            for (HistoryRow row : bucket.values()) {
                if (!seen.contains(row.factKey())) {
                    seen.add(row.factKey());
                }
            }
        }
        return seen.size();
    }

    public long rowsOf(ColocationGroup group) {
        GroupMeta meta = this.groups.get(group);
        return meta == null ? 0 : meta.rows;
    }

    // --------------------------------------------------------------- helpers

    private HistoryRow findRow(ColocationGroup group, long validFrom) {
        for (HistoryRow row : historyOf(group)) {
            if (row.validFrom() == validFrom) {
                return row;
            }
        }
        return null;
    }

    private byte[] fingerprintOf(Request req) {
        return TieBreakers.sha256(TieBreakers.concat(
                req.graphId().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                req.temporalLabel().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                req.entityId().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                req.factKey().canonicalBytes(),
                req.canonicalIntervalPayload()));
    }

    private ByteKey prefixKey(ColocationGroup group) {
        return new ByteKey(this.codec.groupPrefix(group.graphId(),
                                                  group.temporalLabel(),
                                                  group.entityId(),
                                                  group.factKey()));
    }

    private PrefixSpace space(ColocationGroup group) {
        return this.keySpace.computeIfAbsent(prefixKey(group),
                                             k -> new PrefixSpace());
    }

    private GroupMeta meta(ColocationGroup group) {
        return this.groups.computeIfAbsent(group, k -> new GroupMeta());
    }

    private static byte[] longBytes(long v) {
        byte[] out = new byte[8];
        for (int i = 7; i >= 0; i--) {
            out[7 - i] = (byte) ((v >>> (i * 8)) & 0xFF);
        }
        return out;
    }

    private static final class PrefixSpace {

        final Map<Long, NavigableMap<byte[], HistoryRow>> buckets =
                new TreeMap<>();
        final NavigableMap<byte[], OpenIntervalEntry> openIndex =
                new TreeMap<>(BYTES);

        NavigableMap<byte[], HistoryRow> bucket(long bucket) {
            return this.buckets.computeIfAbsent(bucket,
                                                k -> new TreeMap<>(BYTES));
        }

        HistoryRow findByRowKey(byte[] rowKey) {
            for (NavigableMap<byte[], HistoryRow> b : this.buckets.values()) {
                HistoryRow row = b.get(rowKey);
                if (row != null) {
                    return row;
                }
            }
            return null;
        }
    }

    private static final class GroupMeta {

        CurrentPointer current;
        long rows;
        long bytes;
    }

    private static final class CommittedMutation {

        final byte[] fingerprint;
        final long revision;
        final byte[] historyRowKey;
        final String tieBreaker;

        CommittedMutation(byte[] fingerprint, long revision,
                          byte[] historyRowKey, String tieBreaker) {
            this.fingerprint = fingerprint;
            this.revision = revision;
            this.historyRowKey = historyRowKey;
            this.tieBreaker = tieBreaker;
        }
    }

    private static final class ByteKey {

        private final byte[] bytes;

        ByteKey(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ByteKey &&
                   Arrays.equals(this.bytes, ((ByteKey) o).bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(this.bytes);
        }
    }
}
