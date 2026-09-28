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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.util.HgStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Store-side temporal apply handler. It is called by the PartitionStateMachine apply thread. */
public final class TemporalMutationHandler implements RaftTaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TemporalMutationHandler.class);

    public static final byte TEMPORAL_MUTATION = 0x6A;
    private static final byte LEDGER_PREFIX = 0x01;
    private static final ConcurrentMap<String, Object> FACT_LOCKS = new ConcurrentHashMap<>();
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
                    Arrays.copyOfRange(request, 1, request.length)), applyIndex);
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
        Object factLock = factLock(bundle);
        synchronized (factLock) {
            // Instrumentation (Slice 1 condition 3): resolve the RocksDB instance that
            // backs this partition's writes so the verifier can prove the write and the
            // conflict-scan read hit the SAME store instance (no split-brain). Borrowed
            // with the same try-with-resources discipline as doGet -> close() is a
            // refcount return, not a DB shutdown.
            String rocksId;
            try (RocksDBSession db = businessHandler.getSession(groupId)) {
                rocksId = Integer.toHexString(System.identityHashCode(db)) + "@" + db.getDbPath();
            }
            LOG.info("temporal apply entry handlerId={} businessId={} rocksdb={} thread={} " +
                     "groupId={} mutationId={} factKey={} code={} operation={} " +
                     "interval=[{},{}] index={}",
                     System.identityHashCode(this), System.identityHashCode(businessHandler),
                     rocksId, Thread.currentThread().getName(), groupId, bundle.mutationId(),
                     new String(bundle.factKey()), code, bundle.operation(),
                     bundle.validFrom(), bundle.validTo(), applyIndex);
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
                    LOG.info("temporal apply committed handlerId={} businessId={} thread={} " +
                             "groupId={} mutationId={} index={} views={}",
                             System.identityHashCode(this),
                             System.identityHashCode(businessHandler),
                             Thread.currentThread().getName(), groupId,
                             bundle.mutationId(), applyIndex, bundle.views().size());
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
            LOG.warn("temporal conflict decision handlerId={} businessId={} " +
                     "mutationId={} index={}", System.identityHashCode(this),
                     System.identityHashCode(businessHandler),
                     bundle.mutationId(), applyIndex);
            throw new HgStoreException(HgStoreException.EC_TEMPORAL_CONFLICT,
                                       "TEMPORAL_CONFLICT for fact key");
        }

        LOG.info("temporal conflict-clear handlerId={} businessId={} " +
                 "mutationId={} index={}", System.identityHashCode(this),
                 System.identityHashCode(businessHandler),
                 bundle.mutationId(), applyIndex);
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

    /** Fact-scoped marker lookup by {@code valid_from}, skipping tombstones. */
    private FoundInterval findInterval(String graph, int code, byte[] factKey,
                                       long validFrom) {
        try (ScanIterator iterator = businessHandler.scanPrefix(
                graph, code, HugeServerTables.TEMPORAL_HISTORY_TABLE, factKey)) {
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

    private boolean hasConflict(TemporalMutationBundle bundle, int code) {
        // D-1 (replica-divergence red line). BusinessHandler.scan* treats its code
        // parameter as a key-hash code, not a partition id, so passing groupId here
        // would resolve to the wrong partition and silently miss conflicts.
        //
        // SCAN_ALL_PARTITIONS_ID is NOT a valid remedy either: BusinessHandlerImpl
        // maps code == -1 to getLeaderPartitionIds(graph), which filters on
        // Partition.isLeader(). On a follower the owning partition is therefore
        // excluded from the scan list, the conflict scan reads zero rows,
        // hasConflict() returns false, and the follower COMMITS a mutation that the
        // leader deterministically REJECTED. That makes the Raft apply
        // non-deterministic and silently diverges the replicas (observed: groupId=6
        // index=4059, hg-store1 rejected mutation-b while hg-store0/hg-store2
        // committed it).
        //
        // The fact-key hash is the same code already used by the ledger doGet() and
        // by every tx.put() below; it resolves through
        // pdProvider.getPartitionByCode(), which is PD metadata and independent of
        // leadership. Read path == write path == identical on every replica.
        byte[] prefix = bundle.factKey();
        try (ScanIterator iterator = businessHandler.scanPrefix(
                bundle.graph(), code,
                HugeServerTables.TEMPORAL_HISTORY_TABLE, prefix)) {
            int scanned = 0;
            while (iterator.hasNext()) {
                RocksDBSession.BackendColumn column = iterator.next();
                scanned++;
                byte[] key = column.name;
                LOG.debug("temporal conflict-scan key len={} headHex={}", key.length,
                          toHex(key, 0, Math.min(8, key.length)));
                TemporalIntervalCodec.Interval interval =
                        TemporalIntervalCodec.parse(key, prefix);
                if (interval == null) {
                    // ledger row or a fact-key hash collision; never fold it in.
                    continue;
                }
                if (TemporalIntervalCodec.state(column.value) ==
                    TemporalIntervalCodec.STATE_TOMBSTONE) {
                    // A deleted interval no longer participates in conflict checks.
                    continue;
                }
                long from = interval.validFrom;
                long to = interval.open ? Long.MAX_VALUE : interval.validTo;
                long candidateTo = bundle.open() ? Long.MAX_VALUE : bundle.validTo();
                long existingTo = to == Long.MAX_VALUE ? Long.MAX_VALUE : to;
                if (bundle.validFrom() < existingTo && from < candidateTo) {
                    LOG.warn("temporal conflict detected factKey={} from={} to={} " +
                             "candidate=[{},{}) scanned={}",
                             new String(bundle.factKey()), from, to,
                             bundle.validFrom(), candidateTo, scanned);
                    return true;
                }
            }
            LOG.debug("temporal conflict-scan done scanned={}", scanned);
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
     * Per-fact interned lock guarding the standalone apply path against a
     * concurrent same-fact apply on this replica (leader vs. follower thread).
     * Keyed by graph + fact key, identical to the frozen Slice 1 lock key.
     */
    private static Object factLock(TemporalMutationBundle bundle) {
        String lockKey = bundle.graph() + "\\u0000" + new String(bundle.factKey(),
                                                                  StandardCharsets.UTF_8);
        return FACT_LOCKS.computeIfAbsent(lockKey, key -> new Object());
    }

    private static byte[] ledgerKey(String mutationId) {
        byte[] id = mutationId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + id.length);
        buffer.put(LEDGER_PREFIX).put(id);
        return buffer.array();
    }

    private static String toHex(byte[] bytes, int off, int len) {
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = off; i < off + len; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }
        return sb.toString();
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
