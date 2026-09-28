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

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Store-side temporal apply handler. It is called by the PartitionStateMachine apply thread. */
public final class TemporalMutationHandler implements RaftTaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TemporalMutationHandler.class);

    public static final byte TEMPORAL_MUTATION = 0x6A;
    private static final byte LEDGER_PREFIX = 0x01;

    /**
     * Fixed-size striped locks of {@link #factLock}: bounded memory instead of
     * the previous unbounded per-fact intern map (one entry per distinct fact
     * key ever written, a slow leak on a long-lived Store).
     */
    private static final int FACT_LOCK_STRIPES = 1024;
    private static final Object[] FACT_LOCKS = new Object[FACT_LOCK_STRIPES];

    static {
        for (int i = 0; i < FACT_LOCKS.length; i++) {
            FACT_LOCKS[i] = new Object();
        }
    }

    /**
     * Bucket-walk cap of the pruned conflict scan; beyond it the legacy full
     * fact-sequence walk runs as the correctness fallback.
     */
    private static final int CONFLICT_SCAN_MAX_BUCKETS = 512;

    /** Status bits returned by the conflict-scan helpers. */
    private static final int SCAN_CONFLICT = 1;
    private static final int SCAN_ACTIVE = 2;
    private static final int SCAN_PAST_CANDIDATE = 4;

    private final BusinessHandler businessHandler;

    public TemporalMutationHandler(BusinessHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    @Override
    public boolean invoke(int groupId, byte[] request, RaftClosure response,
                          long applyIndex) throws HgStoreException {
        if (request.length == 0 || request[0] != TEMPORAL_MUTATION) {
            return false;
        }
        try {
            apply(groupId, TemporalMutationBundleCodec.decode(
                    request, 1, request.length - 1), applyIndex);
        } catch (IOException e) {
            throw unsupportedVersion(request[0], e);
        }
        return true;
    }

    @Override
    public boolean invoke(int groupId, byte[] request, RaftClosure response)
            throws HgStoreException {
        throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                   "temporal apply requires Raft apply index");
    }

    @Override
    public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response,
                          long applyIndex) throws HgStoreException {
        if (methodId != TEMPORAL_MUTATION) {
            return false;
        }
        if (!(req instanceof TemporalMutationBundle)) {
            throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                       "temporal request must be a bundle");
        }
        apply(groupId, (TemporalMutationBundle) req, applyIndex);
        return true;
    }

    @Override
    public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response)
            throws HgStoreException {
        throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                   "temporal apply requires Raft apply index");
    }

    private void apply(int groupId, TemporalMutationBundle bundle, long applyIndex)
            throws HgStoreException {
        if (applyIndex <= 0) {
            throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                       "temporal apply requires positive Raft index");
        }
        // BusinessHandler treats its code parameter as a key-hash code: it is
        // used both to resolve the owning partition and as the 2-byte suffix of
        // the stored key. It must therefore be the fact-key hash (which by
        // definition falls inside this partition's key range), not the partition
        // id (groupId). Using groupId made writes land under a suffix that
        // reads/conflict-scans could not reproduce.
        int code = PartitionUtils.calcHashcode(bundle.factKey());
        byte[] ledgerKey = ledgerKey(bundle.mutationId());
        synchronized (factLock(bundle.graph(), code)) {
            if (businessHandler.doGet(bundle.graph(), code,
                                      HugeServerTables.TEMPORAL_HISTORY_TABLE,
                                      ledgerKey) != null) {
                LOG.info("temporal apply ledger-hit no-op groupId={} mutationId={}",
                         groupId, bundle.mutationId());
                return;
            }
            // Standalone temporal apply owns its Store transaction. The atomic
            // batch path (Phase B) instead reuses contribute() to write into the
            // caller's shared transaction so a normal write and its bound temporal
            // mutation commit as one Store transaction.
            BusinessHandler.TxBuilder tx = businessHandler.txBuilder(bundle.graph(), groupId);
            try {
                if (contributeOp(tx, bundle, code, ledgerKey, applyIndex)) {
                    tx.build().commit();
                    LOG.info("temporal apply committed groupId={} mutationId={} " +
                             "index={} operation={} views={}",
                             groupId, bundle.mutationId(), applyIndex,
                             bundle.operation(), bundle.views().size());
                } else {
                    // Idempotent no-op (already-closed interval): nothing to persist.
                    tx.build().rollback();
                    LOG.info("temporal apply no-op rollback groupId={} mutationId={} index={}",
                             groupId, bundle.mutationId(), applyIndex);
                }
            } catch (RuntimeException e) {
                tx.build().rollback();
                throw e;
            }
        }
    }

    /**
     * Contribute a temporal mutation's view writes into an externally-owned
     * Store transaction WITHOUT committing it (Phase B atomic commit). Used by
     * the batch path where a normal mutation and its bound temporal mutation
     * must land in a single Raft proposal / Store transaction.
     *
     * <p>Pre-checks (ledger idempotence and the interval conflict scan) run
     * here, before any write, so a rejected or already-applied mutation
     * contributes nothing and never forces the caller to discard co-committed
     * normal writes. Returns {@code true} when at least one write was
     * contributed (the caller must commit); {@code false} for a no-op (ledger
     * hit or idempotent close).
     *
     * <p>The Raft apply thread serialises applies per partition and a single
     * fact key maps to a single partition, so no extra fact lock is taken here.
     */
    public boolean contribute(int groupId, TemporalMutationBundle bundle,
                              BusinessHandler.TxBuilder tx, long applyIndex)
            throws HgStoreException {
        if (applyIndex <= 0) {
            throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                       "temporal apply requires positive Raft index");
        }
        int code = PartitionUtils.calcHashcode(bundle.factKey());
        byte[] ledgerKey = ledgerKey(bundle.mutationId());
        if (businessHandler.doGet(bundle.graph(), code,
                                  HugeServerTables.TEMPORAL_HISTORY_TABLE,
                                  ledgerKey) != null) {
            LOG.info("temporal contribute ledger-hit no-op groupId={} mutationId={}",
                     groupId, bundle.mutationId());
            return false;
        }
        return contributeOp(tx, bundle, code, ledgerKey, applyIndex);
    }

    /**
     * Dispatch the operation-specific writes into {@code tx} (no commit). Runs
     * the operation's pre-checks first. Returns whether anything was written;
     * {@code false} means an idempotent no-op that leaves {@code tx} untouched.
     */
    private boolean contributeOp(BusinessHandler.TxBuilder tx, TemporalMutationBundle bundle,
                                 int code, byte[] ledgerKey, long applyIndex)
            throws HgStoreException {
        switch (bundle.operation()) {
            case APPEND:
            case UPSERT:
                return writeInterval(tx, bundle, code, ledgerKey, applyIndex);
            case CLOSE:
                return writeClose(tx, bundle, code, ledgerKey, applyIndex);
            case DELETE:
                return writeDelete(tx, bundle, code, ledgerKey, applyIndex);
            default:
                throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                           "unknown temporal operation: " +
                                           bundle.operation());
        }
    }

    /**
     * Interval-creating write (APPEND/UPSERT): conflict scan, then the four
     * views, the ledger and the interval marker into the shared transaction
     * (design ruling §3.2). Identical writes to the frozen Slice 1 path; only
     * the commit boundary moved to the caller so it can be shared with a bound
     * normal mutation.
     */
    private boolean writeInterval(BusinessHandler.TxBuilder tx, TemporalMutationBundle bundle,
                                  int code, byte[] ledgerKey, long applyIndex)
            throws HgStoreException {
        if (hasConflict(bundle, code)) {
            LOG.warn("temporal conflict decision mutationId={} index={}",
                     bundle.mutationId(), applyIndex);
            throw new HgStoreException(HgStoreException.EC_TEMPORAL_CONFLICT,
                                       "TEMPORAL_CONFLICT for fact key");
        }

        byte[] revisionValue = TemporalIntervalCodec.value(bundle.payload(), applyIndex);
        for (TemporalMutationBundle.ViewMutation view : bundle.views()) {
            String table = table(view.name());
            tx.put(code, table, view.key(), revisionValue);
        }
        // The ledger and interval marker are part of this same Store transaction.
        tx.put(code, HugeServerTables.TEMPORAL_HISTORY_TABLE,
               ledgerKey, revisionValue);
        tx.put(code, HugeServerTables.TEMPORAL_HISTORY_TABLE,
               TemporalIntervalCodec.intervalKey(bundle), revisionValue);
        // Phase C: a bound bundle also maintains the element-index in this SAME
        // transaction (same code), so it commits atomically with the interval
        // views. An unbound bundle writes nothing here (current-only).
        putElementIndex(tx, bundle, code, bundle.validFrom(),
                        bundle.open() ? null : bundle.validTo(), applyIndex,
                        TemporalIntervalCodec.STATE_ACTIVE);
        return true;
    }

    /**
     * CLOSE write (design ruling §3.2): locate the open interval marker and, in
     * the shared transaction, replace it with a closed marker
     * ({@code valid_to = close_time}) while preserving the payload, then record
     * the ledger. Closing an already-closed interval to the same time is an
     * idempotent no-op (returns {@code false}); closing it to a different time
     * is a conflict.
     */
    private boolean writeClose(BusinessHandler.TxBuilder tx, TemporalMutationBundle bundle,
                               int code, byte[] ledgerKey, long applyIndex)
            throws HgStoreException {
        long closeAt = bundle.validTo();
        FoundInterval found = findInterval(bundle.graph(), code, bundle.factKey(),
                                           bundle.validFrom());
        if (found == null) {
            throw new HgStoreException(HgStoreException.EC_TEMPORAL_CONFLICT,
                                       "TEMPORAL_CONFLICT: no interval to close at " +
                                       "valid_from=" + bundle.validFrom());
        }
        if (!found.open()) {
            if (found.validTo != null && found.validTo == closeAt) {
                LOG.info("temporal close idempotent no-op mutationId={} " +
                         "validFrom={} closeAt={}", bundle.mutationId(),
                         bundle.validFrom(), closeAt);
                return false;
            }
            throw new HgStoreException(HgStoreException.EC_TEMPORAL_CLOSED_INTERVAL_CONFLICT,
                                       "TEMPORAL_CLOSED_INTERVAL_CONFLICT: interval already " +
                                       "closed at " + found.validTo);
        }
        byte[] payload = TemporalIntervalCodec.payload(found.value);
        byte[] closedKey = TemporalIntervalCodec.intervalKey(bundle.factKey(),
                                                             bundle.validFrom(), closeAt);
        tx.del(code, HugeServerTables.TEMPORAL_HISTORY_TABLE, found.key);
        tx.put(code, HugeServerTables.TEMPORAL_HISTORY_TABLE, closedKey,
               TemporalIntervalCodec.value(payload, applyIndex));
        tx.put(code, HugeServerTables.TEMPORAL_HISTORY_TABLE, ledgerKey,
               TemporalIntervalCodec.value(bundle.payload(), applyIndex));
        // Phase C: a bound close moves the element-index entry from open to
        // [valid_from, close_at) in the same transaction, keeping it consistent
        // with the closed interval marker. Unbound: no-op.
        if (bundle.hasElementBinding()) {
            delElementIndex(tx, bundle, code, bundle.validFrom(), null);
            putElementIndex(tx, bundle, code, bundle.validFrom(), closeAt, applyIndex,
                            TemporalIntervalCodec.STATE_ACTIVE);
        }
        return true;
    }

    /**
     * DELETE write (design ruling §3.2): mark the interval marker tombstone in
     * place (same key, state byte), preserving the payload for audit, and record
     * the ledger. The temporal read path filters tombstones; the history view
     * retains the deleted record for audit.
     */
    private boolean writeDelete(BusinessHandler.TxBuilder tx, TemporalMutationBundle bundle,
                                int code, byte[] ledgerKey, long applyIndex)
            throws HgStoreException {
        FoundInterval found = findInterval(bundle.graph(), code, bundle.factKey(),
                                           bundle.validFrom());
        if (found == null) {
            throw new HgStoreException(HgStoreException.EC_TEMPORAL_CONFLICT,
                                       "TEMPORAL_CONFLICT: no interval to delete at " +
                                       "valid_from=" + bundle.validFrom());
        }
        byte[] payload = TemporalIntervalCodec.payload(found.value);
        tx.put(code, HugeServerTables.TEMPORAL_HISTORY_TABLE, found.key,
               TemporalIntervalCodec.value(payload, applyIndex,
                                           TemporalIntervalCodec.STATE_TOMBSTONE));
        tx.put(code, HugeServerTables.TEMPORAL_HISTORY_TABLE, ledgerKey,
               TemporalIntervalCodec.value(bundle.payload(), applyIndex));
        // Phase C: a bound delete tombstones the element-index entry in place
        // (same key as the interval it mirrors), so the read path filters it.
        // Unbound: no-op.
        if (bundle.hasElementBinding()) {
            tx.put(code, HugeServerTables.TEMPORAL_ELEMENT_INDEX_TABLE,
                   elementKey(bundle, found.validFrom, found.validTo),
                   TemporalIntervalCodec.value(bundle.factKey(), applyIndex,
                                               TemporalIntervalCodec.STATE_TOMBSTONE));
        }
        return true;
    }

    /**
     * Phase C: build the element-index key for a bound bundle's interval. The
     * caller guarantees {@code bundle.hasElementBinding()}. {@code validTo ==
     * null} means open and maps to {@link TemporalElementIndexCodec#OPEN_VALID_TO}.
     */
    private static byte[] elementKey(TemporalMutationBundle bundle, long validFrom,
                                     Long validTo) {
        long to = validTo == null ? TemporalElementIndexCodec.OPEN_VALID_TO : validTo;
        return TemporalElementIndexCodec.elementIndexKey(
                bundle.elementKind().code(),
                bundle.elementId().getBytes(StandardCharsets.UTF_8),
                validFrom, to);
    }

    /**
     * Phase C: write the element-index entry of a bound bundle into the shared
     * transaction (same code as the interval views, so it commits atomically).
     * A no-op when the bundle carries no element binding, which keeps an unbound
     * temporal write byte-for-byte identical to the pre-Phase-C path. The value
     * carries the canonical fact key so a reader can hop back to the fact
     * sequence; {@code state} mirrors the interval marker state.
     */
    private void putElementIndex(BusinessHandler.TxBuilder tx, TemporalMutationBundle bundle,
                                 int code, long validFrom, Long validTo, long applyIndex,
                                 byte state) {
        if (!bundle.hasElementBinding()) {
            return;
        }
        tx.put(code, HugeServerTables.TEMPORAL_ELEMENT_INDEX_TABLE,
               elementKey(bundle, validFrom, validTo),
               TemporalIntervalCodec.value(bundle.factKey(), applyIndex, state));
    }

    /**
     * Phase C: remove the element-index entry of a bound bundle (used by CLOSE to
     * move an open entry to its closed replacement). A no-op when unbound.
     */
    private void delElementIndex(BusinessHandler.TxBuilder tx, TemporalMutationBundle bundle,
                                 int code, long validFrom, Long validTo) {
        if (!bundle.hasElementBinding()) {
            return;
        }
        tx.del(code, HugeServerTables.TEMPORAL_ELEMENT_INDEX_TABLE,
               elementKey(bundle, validFrom, validTo));
    }

    /**
     * Fact-scoped marker lookup by {@code valid_from}, skipping tombstones. An
     * active marker of one {@code valid_from} can only live in two buckets: the
     * open sentinel bucket (an open marker carries OPEN_BUCKET regardless of
     * its start) or its own start bucket (a closed marker). Both are
     * single-bucket seeks instead of a full fact-sequence scan; the open-bucket
     * probe comes first, matching the key order of the previous full walk.
     */
    private FoundInterval findInterval(String graph, int code, byte[] factKey,
                                       long validFrom) {
        FoundInterval found = findIntervalInBucket(graph, code, factKey,
                                                   TemporalIntervalCodec.OPEN_BUCKET,
                                                   validFrom);
        if (found != null) {
            return found;
        }
        return findIntervalInBucket(graph, code, factKey,
                                    TemporalIntervalCodec.bucketOf(validFrom), validFrom);
    }

    /** Marker lookup by {@code valid_from} within one bucket. */
    private FoundInterval findIntervalInBucket(String graph, int code, byte[] factKey,
                                               long bucket, long validFrom) {
        byte[] prefix = TemporalIntervalCodec.bucketPrefix(factKey, bucket);
        try (ScanIterator iterator = businessHandler.scanPrefix(
                graph, code, HugeServerTables.TEMPORAL_HISTORY_TABLE, prefix)) {
            while (iterator.hasNext()) {
                RocksDBSession.BackendColumn column = iterator.next();
                TemporalIntervalCodec.Interval interval =
                        TemporalIntervalCodec.parse(column.name, factKey);
                if (interval == null || interval.validFrom != validFrom) {
                    continue;
                }
                byte state = TemporalIntervalCodec.state(column.value);
                if (state == TemporalIntervalCodec.STATE_TOMBSTONE) {
                    continue;
                }
                return new FoundInterval(column.name, column.value,
                                         interval.validFrom, interval.validTo);
            }
        }
        return null;
    }

    /**
     * Conflict scan of an interval-creating write (APPEND/UPSERT), pruned to the
     * marker buckets that can hold a conflicting active marker. A candidate
     * {@code [valid_from, valid_to)} conflicts with an active marker
     * {@code [from, to)} iff {@code valid_from < to && from < valid_to}, the same
     * half-open rule as the frozen Slice 1 full walk:
     *
     * <ul>
     *   <li>Phase A: one probe of the open sentinel bucket, where every open
     *       marker lives.</li>
     *   <li>Phase B: one ordered range scan from the candidate's own bucket to
     *       the bucket after the candidate end (or the sequence end for an open
     *       candidate). It also covers same-bucket markers that start before the
     *       candidate start.</li>
     *   <li>Phase C: at most {@link #CONFLICT_SCAN_MAX_BUCKETS} single-bucket
     *       probes walking down to the first bucket holding an active marker;
     *       that marker decides (overlapping -> conflict, otherwise the
     *       non-overlap invariant proves every earlier marker ends at or before
     *       it and cannot cross). A first-row probe skips both the walk and the
     *       fallback when nothing lives below the candidate's bucket; the legacy
     *       full walk runs as the correctness fallback only when older markers
     *       may exist.</li>
     * </ul>
     *
     * <p>D-1 (replica-divergence red line). {@code BusinessHandler.scan*} treats
     * its code parameter as a key-hash code, not a partition id, so passing
     * groupId here would resolve to the wrong partition and silently miss
     * conflicts. {@code SCAN_ALL_PARTITIONS_ID} is NOT a valid remedy either:
     * BusinessHandlerImpl maps code == -1 to getLeaderPartitionIds(graph), which
     * filters on Partition.isLeader(). On a follower the owning partition is
     * therefore excluded from the scan list, the conflict scan reads zero rows,
     * hasConflict() returns false, and the follower COMMITS a mutation that the
     * leader deterministically REJECTED. That makes the Raft apply
     * non-deterministic and silently diverges the replicas (observed: groupId=6
     * index=4059, hg-store1 rejected mutation-b while hg-store0/hg-store2
     * committed it). Every scan below passes the fact-key hash, the same code
     * already used by the ledger doGet() and by every tx.put() of the write
     * path; it resolves through pdProvider.getPartitionByCode(), which is PD
     * metadata independent of leadership. Read path == write path == identical
     * on every replica.</p>
     */
    private boolean hasConflict(TemporalMutationBundle bundle, int code) {
        byte[] factKey = bundle.factKey();
        long newFrom = bundle.validFrom();
        long candidateTo = bundle.open() ? Long.MAX_VALUE : bundle.validTo();

        // Phase A: every open marker lives in the OPEN_BUCKET sentinel bucket
        // (its sortable form sorts before all real buckets): one seek decides
        // them all.
        if ((scanMarkerBucket(bundle.graph(), code, factKey,
                              TemporalIntervalCodec.OPEN_BUCKET, newFrom,
                              candidateTo) & SCAN_CONFLICT) != 0) {
            return true;
        }

        // Phase B: one ordered range scan from the candidate's own bucket
        // prefix up to (excluding) the bucket after the candidate end -- or to
        // the end of the fact sequence for an open candidate.
        long fromBucket = TemporalIntervalCodec.bucketOf(newFrom);
        byte[] fromKey = TemporalIntervalCodec.bucketPrefix(factKey, fromBucket);
        byte[] toKey = candidateTo == Long.MAX_VALUE ? null :
                       TemporalIntervalCodec.bucketPrefix(
                               factKey,
                               TemporalIntervalCodec.bucketOf(candidateTo - 1) + 1);
        if (scanMarkerRange(bundle.graph(), code, factKey, fromKey, toKey,
                            newFrom, candidateTo)) {
            return true;
        }

        // Phase C: the remaining conflict candidates start before fromBucket.
        // One first-row probe of the fact sequence decides whether such rows
        // exist at all.
        long windowStart = fromBucket - CONFLICT_SCAN_MAX_BUCKETS;
        byte[] windowStartKey =
                TemporalIntervalCodec.bucketPrefix(factKey, windowStart);
        byte[] firstRow = firstFactRow(bundle.graph(), code, factKey);
        if (firstRow == null || Bytes.compare(firstRow, fromKey) >= 0) {
            // Either the fact sequence is empty, or every row lives in buckets
            // >= fromBucket, which phases A/B already covered; markers starting
            // at/after candidateTo cannot conflict.
            return false;
        }
        // Walk down to the first bucket holding an active marker; that marker
        // decides the whole lower region.
        for (long bucket = fromBucket - 1, walked = 0;
             walked < CONFLICT_SCAN_MAX_BUCKETS; bucket--, walked++) {
            int status = scanMarkerBucket(bundle.graph(), code, factKey, bucket,
                                          newFrom, candidateTo);
            if ((status & SCAN_CONFLICT) != 0) {
                return true;
            }
            if ((status & SCAN_ACTIVE) != 0) {
                // Not overlapping: the non-overlap invariant proves every
                // earlier marker ends at or before this one, hence before
                // newFrom, and cannot cross the candidate.
                return false;
            }
            if ((status & SCAN_PAST_CANDIDATE) != 0) {
                // Defensive: markers below fromBucket cannot start at/after
                // candidateTo; this means the non-overlap invariant is
                // violated, so fall back to the full walk for correctness.
                return legacyHasConflict(bundle, code);
            }
        }
        // A first row at/after windowStart means every row lies inside the
        // window covered by phases A-C; otherwise older markers may exist and
        // the legacy full walk is the correctness fallback.
        if (Bytes.compare(firstRow, windowStartKey) >= 0) {
            return false;
        }
        return legacyHasConflict(bundle, code);
    }

    /**
     * Ordered range scan over the marker buckets of the fact sequence starting
     * at {@code fromKey}; {@code toKey == null} scans up to the end of the
     * sequence. Returns whether an active marker in range overlaps the
     * candidate. All scans pass the fact-key hash code (D-1).
     */
    private boolean scanMarkerRange(String graph, int code, byte[] factKey,
                                    byte[] fromKey, byte[] toKey,
                                    long newFrom, long candidateTo) {
        try (ScanIterator iterator = businessHandler.scan(
                graph, code, HugeServerTables.TEMPORAL_HISTORY_TABLE,
                fromKey, toKey,
                ScanIterator.Trait.SCAN_GTE_BEGIN | ScanIterator.Trait.SCAN_LT_END)) {
            while (iterator.hasNext()) {
                RocksDBSession.BackendColumn column = iterator.next();
                TemporalIntervalCodec.Interval interval =
                        TemporalIntervalCodec.parse(column.name, factKey);
                if (interval == null) {
                    // Ledger row or a fact-key hash collision; never fold in.
                    continue;
                }
                if (interval.validFrom >= candidateTo) {
                    // Rows order by (bucket, valid_from): from here on every
                    // marker starts at/after the candidate end.
                    break;
                }
                if (TemporalIntervalCodec.state(column.value) ==
                    TemporalIntervalCodec.STATE_TOMBSTONE) {
                    // A deleted interval no longer participates.
                    continue;
                }
                if (overlaps(interval, newFrom, candidateTo)) {
                    logConflict(factKey, interval, newFrom, candidateTo);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Single-bucket prefix probe. Returns the marker presence bits of the
     * bucket: any conflict ({@link #SCAN_CONFLICT}), any active (non-tombstoned)
     * marker ({@link #SCAN_ACTIVE}), and any marker starting at/after the
     * candidate end ({@link #SCAN_PAST_CANDIDATE}).
     */
    private int scanMarkerBucket(String graph, int code, byte[] factKey,
                                 long bucket, long newFrom, long candidateTo) {
        byte[] prefix = TemporalIntervalCodec.bucketPrefix(factKey, bucket);
        int status = 0;
        try (ScanIterator iterator = businessHandler.scanPrefix(
                graph, code, HugeServerTables.TEMPORAL_HISTORY_TABLE, prefix)) {
            while (iterator.hasNext()) {
                RocksDBSession.BackendColumn column = iterator.next();
                TemporalIntervalCodec.Interval interval =
                        TemporalIntervalCodec.parse(column.name, factKey);
                if (interval == null) {
                    continue;
                }
                if (interval.validFrom >= candidateTo) {
                    status |= SCAN_PAST_CANDIDATE;
                    break;
                }
                if (TemporalIntervalCodec.state(column.value) ==
                    TemporalIntervalCodec.STATE_TOMBSTONE) {
                    continue;
                }
                status |= SCAN_ACTIVE;
                if (overlaps(interval, newFrom, candidateTo)) {
                    logConflict(factKey, interval, newFrom, candidateTo);
                    return SCAN_CONFLICT;
                }
            }
        }
        return status;
    }

    /** First row of the fact sequence, or {@code null} when it is empty. */
    private byte[] firstFactRow(String graph, int code, byte[] factKey) {
        try (ScanIterator iterator = businessHandler.scanPrefix(
                graph, code, HugeServerTables.TEMPORAL_HISTORY_TABLE, factKey)) {
            if (!iterator.hasNext()) {
                return null;
            }
            RocksDBSession.BackendColumn column = iterator.next();
            return column.name;
        }
    }

    /**
     * Conflict rule of the frozen Slice 1 walk, deliberately left un-simplified
     * to keep the zero-length boundary behavior identical.
     */
    private static boolean overlaps(TemporalIntervalCodec.Interval interval,
                                    long newFrom, long candidateTo) {
        long existingTo = interval.open ? Long.MAX_VALUE : interval.validTo;
        return newFrom < existingTo && interval.validFrom < candidateTo;
    }

    private static void logConflict(byte[] factKey,
                                    TemporalIntervalCodec.Interval interval,
                                    long candidateFrom, long candidateTo) {
        LOG.warn("temporal conflict detected factKey={} from={} to={} " +
                 "candidate=[{},{})",
                 new String(factKey, StandardCharsets.UTF_8), interval.validFrom,
                 interval.open ? Long.MAX_VALUE : interval.validTo,
                 candidateFrom, candidateTo);
    }

    /**
     * Legacy full fact-sequence conflict walk (frozen Slice 1 behavior): the
     * correctness fallback when the pruned scan cannot rule the lower region
     * out. All scans pass the fact-key hash code (D-1).
     */
    private boolean legacyHasConflict(TemporalMutationBundle bundle, int code) {
        byte[] factKey = bundle.factKey();
        long newFrom = bundle.validFrom();
        long candidateTo = bundle.open() ? Long.MAX_VALUE : bundle.validTo();
        try (ScanIterator iterator = businessHandler.scanPrefix(
                bundle.graph(), code, HugeServerTables.TEMPORAL_HISTORY_TABLE,
                factKey)) {
            while (iterator.hasNext()) {
                RocksDBSession.BackendColumn column = iterator.next();
                TemporalIntervalCodec.Interval interval =
                        TemporalIntervalCodec.parse(column.name, factKey);
                if (interval == null) {
                    continue;
                }
                if (TemporalIntervalCodec.state(column.value) ==
                    TemporalIntervalCodec.STATE_TOMBSTONE) {
                    continue;
                }
                if (overlaps(interval, newFrom, candidateTo)) {
                    logConflict(factKey, interval, newFrom, candidateTo);
                    return true;
                }
            }
            return false;
        }
    }

    private static HgStoreException unsupportedVersion(byte op, Throwable cause) {
        return new HgStoreException(HgStoreException.EC_TEMPORAL_UNSUPPORTED_VERSION,
                                    "TEMPORAL_UNSUPPORTED_VERSION op=0x" +
                                    Integer.toHexString(op & 0xFF) + " " +
                                    TemporalWireProtocol.describe(), cause);
    }

    private static String table(String name) {
        if (HugeServerTables.TEMPORAL_HISTORY_TABLE.equals(name) ||
            HugeServerTables.TEMPORAL_CURRENT_TABLE.equals(name) ||
            HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE.equals(name) ||
            HugeServerTables.TEMPORAL_INDEX_TABLE.equals(name)) {
            return name;
        }
        throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                   "unknown temporal view: " + name);
    }

    /**
     * Striped lock guarding the standalone apply path against a concurrent
     * same-fact apply on this replica (leader vs. follower thread). The stripe
     * is derived from the graph and the fact-key partition code, so one fact
     * always maps to one stripe -- the same mutual exclusion as the frozen
     * Slice 1 per-fact lock -- while the lock array stays fixed-size.
     */
    private static Object factLock(String graph, int code) {
        return FACT_LOCKS[Math.floorMod(graph.hashCode() * 31 + code,
                                        FACT_LOCK_STRIPES)];
    }

    private static byte[] ledgerKey(String mutationId) {
        byte[] id = mutationId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[1 + id.length];
        key[0] = LEDGER_PREFIX;
        System.arraycopy(id, 0, key, 1, id.length);
        return key;
    }

    /** A located, non-tombstoned interval marker. */
    private static final class FoundInterval {

        final byte[] key;
        final byte[] value;
        final long validFrom;
        /** {@code null} means open (still valid). */
        final Long validTo;

        FoundInterval(byte[] key, byte[] value, long validFrom, Long validTo) {
            this.key = key;
            this.value = value;
            this.validFrom = validFrom;
            this.validTo = validTo;
        }

        boolean open() {
            return this.validTo == null;
        }
    }
}
