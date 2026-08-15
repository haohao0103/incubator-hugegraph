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
import java.util.Comparator;
import java.util.List;

import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.util.HgStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store-side temporal read handler.
 *
 * <p>This is the read counterpart of {@link TemporalMutationHandler}. It answers
 * fact-scoped {@code as_of} / {@code between} / {@code overlap} queries over
 * the bucketed interval markers that the write path persists in
 * {@link HugeServerTables#TEMPORAL_HISTORY_TABLE}. A read is a plain Store seek,
 * not a Raft task: it is invoked directly on the owning partition, using the
 * same fact-key hash ({@link PartitionUtils#calcHashcode}) that the write path
 * uses.</p>
 *
 * <p>Phase 4 (design ruling §4): instead of one full fact-scoped scan, a query
 * now seeks the bucket containing the query time and walks back a bounded
 * number of buckets. Open markers live under a sentinel
 * {@link TemporalIntervalCodec#OPEN_BUCKET} that sorts before every real bucket,
 * so an open interval is found in one seek instead of walking back across every
 * bucket it spans.</p>
 *
 * <p>Correctness invariants:</p>
 * <ul>
 *   <li>fact-scoped: every key is re-checked for exact {@code fact_key} identity;
 *       colliding / non-marker rows are skipped;</li>
 *   <li>half-open intervals: {@code [valid_from, valid_to)}, an open interval
 *       stores {@code valid_to = Long.MAX_VALUE} and is unbounded;</li>
 *   <li>bounded scan: the number of scanned rows is capped (raises
 *       {@code TEMPORAL_QUERY_LIMIT_EXCEEDED}), the number of walked buckets is
 *       capped, and a range spanning more buckets than the cap is rejected;</li>
 *   <li>tombstones: deleted intervals are retained for audit but filtered out of
 *       the valid timeline.</li>
 * </ul>
 */
public final class TemporalQueryHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TemporalQueryHandler.class);

    private static final long DEFAULT_MAX_SCAN_ROWS = 100_000L;

    /** ~19 years of 7-day buckets, the bound on the cross-bucket walk-back. */
    private static final long DEFAULT_MAX_BUCKETS = 1024L;

    private final BusinessHandler businessHandler;
    private final long maxScanRows;
    private final long maxBuckets;

    public TemporalQueryHandler(BusinessHandler businessHandler) {
        this(businessHandler, DEFAULT_MAX_SCAN_ROWS, DEFAULT_MAX_BUCKETS);
    }

    public TemporalQueryHandler(BusinessHandler businessHandler, long maxScanRows) {
        this(businessHandler, maxScanRows, DEFAULT_MAX_BUCKETS);
    }

    public TemporalQueryHandler(BusinessHandler businessHandler, long maxScanRows,
                                long maxBuckets) {
        if (businessHandler == null) {
            throw new NullPointerException("businessHandler");
        }
        if (maxScanRows <= 0) {
            throw new IllegalArgumentException("maxScanRows must be positive");
        }
        if (maxBuckets <= 0) {
            throw new IllegalArgumentException("maxBuckets must be positive");
        }
        this.businessHandler = businessHandler;
        this.maxScanRows = maxScanRows;
        this.maxBuckets = maxBuckets;
    }

    /**
     * Return the interval(s) containing {@code time}, i.e. satisfying
     * {@code valid_from <= time < valid_to}. Under the write path's non-overlap
     * guarantee there is at most one such interval per fact key.
     */
    public List<TemporalIntervalRow> asOf(String graph, byte[] factKey, long time) {
        int code = PartitionUtils.calcHashcode(factKey);
        ScanBudget budget = new ScanBudget();
        // 1. Open markers live in their own sentinel bucket and are unbounded,
        //    so an open interval containing `time` is found in one seek.
        for (TemporalIntervalRow row : scanBucket(graph, factKey, code,
                                                  TemporalIntervalCodec.OPEN_BUCKET,
                                                  budget)) {
            if (row.validFrom() <= time) {
                return Collections.singletonList(row);
            }
        }
        // 2. Closed markers: seek the bucket containing `time` and walk back.
        //    Once a bucket's latest marker ends at or before `time`, no earlier
        //    interval can contain `time` (intervals are non-overlapping and
        //    ordered), so the walk-back stops early.
        long targetBucket = TemporalIntervalCodec.bucketOf(time);
        for (long bucket = targetBucket; bucket > targetBucket - this.maxBuckets;
             bucket--) {
            TemporalIntervalRow candidate = null;
            for (TemporalIntervalRow row : scanBucket(graph, factKey, code, bucket,
                                                      budget)) {
                if (row.validFrom() > time) {
                    break; // later markers start after `time`
                }
                candidate = row; // latest marker with valid_from <= time
            }
            if (candidate == null) {
                continue; // no marker with valid_from <= time in this bucket
            }
            if (candidate.open() || time < candidate.validTo()) {
                return Collections.singletonList(candidate);
            }
            // candidate ends at/before time; no earlier interval contains time.
            return Collections.emptyList();
        }
        return Collections.emptyList();
    }

    /**
     * Return intervals that intersect {@code [from, to)}, i.e. satisfying
     * {@code valid_from < to && from < valid_to}.
     */
    public List<TemporalIntervalRow> between(String graph, byte[] factKey,
                                             long from, long to) {
        return range(graph, factKey, from, to);
    }

    /**
     * Alias of {@link #between} matching the frozen contract vocabulary; both
     * are fact-scoped interval intersection queries in the current model.
     */
    public List<TemporalIntervalRow> overlap(String graph, byte[] factKey,
                                             long from, long to) {
        return range(graph, factKey, from, to);
    }

    private List<TemporalIntervalRow> range(String graph, byte[] factKey,
                                            long from, long to) {
        if (from >= to) {
            return Collections.emptyList();
        }
        int code = PartitionUtils.calcHashcode(factKey);
        ScanBudget budget = new ScanBudget();
        List<TemporalIntervalRow> result = new ArrayList<>();
        // 1. Open markers: [a, open) intersects [from, to) iff a < to.
        for (TemporalIntervalRow row : scanBucket(graph, factKey, code,
                                                  TemporalIntervalCodec.OPEN_BUCKET,
                                                  budget)) {
            if (row.validFrom() < to) {
                result.add(row);
            }
        }
        // 2. Closed markers whose valid_from falls inside [from, to).
        long fromBucket = TemporalIntervalCodec.bucketOf(from);
        long toBucket = TemporalIntervalCodec.bucketOf(to - 1);
        if (toBucket - fromBucket + 1 > this.maxBuckets) {
            throw limitExceeded();
        }
        for (long bucket = fromBucket; bucket <= toBucket; bucket++) {
            for (TemporalIntervalRow row : scanBucket(graph, factKey, code, bucket,
                                                      budget)) {
                if (row.validFrom() < to && from < row.validTo()) {
                    result.add(row);
                }
            }
        }
        // 3. Closed markers starting before `from` that span into [from, to).
        //    Only the latest interval of an earlier bucket can span `from`
        //    (every earlier interval ends before it starts), so stop at the
        //    first non-empty earlier bucket.
        for (long bucket = fromBucket - 1; bucket > fromBucket - 1 - this.maxBuckets;
             bucket--) {
            List<TemporalIntervalRow> earlier = scanBucket(graph, factKey, code, bucket,
                                                           budget);
            if (earlier.isEmpty()) {
                continue;
            }
            TemporalIntervalRow latest = earlier.get(earlier.size() - 1);
            if (latest.validFrom() < to && (latest.open() || from < latest.validTo())) {
                result.add(latest);
            }
            break;
        }
        result.sort(Comparator.comparingLong(TemporalIntervalRow::validFrom));
        return result;
    }

    /** Scan one bucket (or the open sentinel bucket) of one fact sequence. */
    private List<TemporalIntervalRow> scanBucket(String graph, byte[] factKey, int code,
                                                 long bucket, ScanBudget budget) {
        byte[] prefix = TemporalIntervalCodec.bucketPrefix(factKey, bucket);
        List<TemporalIntervalRow> rows = new ArrayList<>();
        try (ScanIterator iterator = this.businessHandler.scanPrefix(
                graph, code, HugeServerTables.TEMPORAL_HISTORY_TABLE, prefix)) {
            while (iterator.hasNext()) {
                RocksDBSession.BackendColumn column = iterator.next();
                budget.checkRow();
                TemporalIntervalCodec.Interval interval =
                        TemporalIntervalCodec.parse(column.name, factKey);
                if (interval == null) {
                    // Fact-key hash collision or a non-marker row (ledger).
                    continue;
                }
                if (TemporalIntervalCodec.state(column.value) ==
                    TemporalIntervalCodec.STATE_TOMBSTONE) {
                    // Retained for audit, not part of the valid timeline.
                    continue;
                }
                long revision = TemporalIntervalCodec.revision(column.value);
                rows.add(new TemporalIntervalRow(factKey, interval.validFrom,
                                                 interval.validTo, revision));
            }
        }
        rows.sort(Comparator.comparingLong(TemporalIntervalRow::validFrom));
        LOG.debug("temporal query bucket-scan graph={} factKey={} bucket={} rows={}",
                  graph, new String(factKey, java.nio.charset.StandardCharsets.UTF_8),
                  bucket, rows.size());
        return rows;
    }

    /** Row budget shared across all buckets of one query. */
    private final class ScanBudget {

        private long rows;

        void checkRow() {
            if (++this.rows > TemporalQueryHandler.this.maxScanRows) {
                throw limitExceeded();
            }
        }
    }

    private HgStoreException limitExceeded() {
        return new HgStoreException(
                HgStoreException.EC_TEMPORAL_QUERY_LIMIT_EXCEEDED,
                "TEMPORAL_QUERY_LIMIT_EXCEEDED: maxScanRows=" + this.maxScanRows +
                ", maxBuckets=" + this.maxBuckets);
    }
}
