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

package org.apache.hugegraph.unit.temporal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.temporal.store.ColocationGroup;
import org.apache.hugegraph.temporal.store.ColocationPlacement;
import org.apache.hugegraph.temporal.store.TemporalBackendStoreSkeleton;
import org.apache.hugegraph.temporal.store.TemporalErrorCode;
import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.temporal.store.TemporalRowKeyCodec;
import org.apache.hugegraph.temporal.store.TemporalStoreException;
import org.apache.hugegraph.temporal.store.TemporalWrite;
import org.apache.hugegraph.temporal.store.TieBreakers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Eight targets of the TemporalBackendStore skeleton.
 *
 * These targets only cover cluster independent invariants of the frozen design
 * ruling. They are NOT checkpoint 3 evidence and produce no throughput,
 * latency, conflict-rate or fault-recovery conclusion.
 */
public class TemporalBackendStoreSkeletonTest {

    private static final String GRAPH = "hugegraph";
    private static final String LABEL = "driver_order_rel";
    private static final String ENTITY = "driver_1001";

    private static final long DAY = 24L * 3600 * 1000;
    private static final long WEEK = 7 * DAY;

    private static final List<String> DIMS =
            Arrays.asList("subject_id", "object_id", "relation_type");

    private static TemporalFactKey fact(String subject, String object,
                                        String relation) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("subject_id", subject);
        values.put("object_id", object);
        values.put("relation_type", relation);
        return TemporalFactKey.of(DIMS, values);
    }

    private static TemporalBackendStoreSkeleton store() {
        return store(new TemporalRowKeyCodec(8, WEEK), 1000, 1 << 20);
    }

    private static TemporalBackendStoreSkeleton store(TemporalRowKeyCodec codec,
                                                      long maxRows,
                                                      long maxBytes) {
        return new TemporalBackendStoreSkeleton(codec,
                                                new ColocationPlacement(3),
                                                maxRows, maxBytes);
    }

    private static ColocationGroup group(TemporalFactKey factKey) {
        return new ColocationGroup(GRAPH, LABEL, ENTITY, factKey);
    }

    private static void log(String target, String line) {
        System.out.println("[" + target + "] " + line);
    }

    // ------------------------------------------------------------------- A1

    /**
     * Guard invariant: the error code enum is exactly the closed set frozen by
     * Phase 0 contract section 5.3, no synonym and no extra code.
     */
    @Test
    public void a1RegisteredErrorCodeClosedSet() {
        List<String> registered = Arrays.asList(
                "IDEMPOTENCY_CONFLICT",
                "TEMPORAL_CONFLICT",
                "TEMPORAL_CROSS_REGION_UNSUPPORTED",
                "TEMPORAL_QUERY_LIMIT_EXCEEDED",
                "TEMPORAL_UNSUPPORTED_VERSION",
                "UNKNOWN_TEMPORAL_SCHEMA",
                "TEMPORAL_CLOSED_INTERVAL_CONFLICT",
                "TEMPORAL_COLOCATION_CAPACITY_EXCEEDED");
        List<String> actual = new ArrayList<>();
        for (TemporalErrorCode code : TemporalErrorCode.values()) {
            actual.add(code.name());
        }
        Assert.assertEquals("error code set must equal contract 5.3",
                            registered, actual);
        log("A1", "error codes = " + actual.size() + " " + actual);
    }

    // ------------------------------------------------------------------- T1

    /**
     * T1 row key encoding and ordering, design section 2.1.
     * - one colocation group shares one contiguous byte prefix
     * - byte order equals numeric valid_from order, including negative millis
     * - equal valid_from falls back to tie_breaker order
     * - bucket ordering never contradicts valid_from ordering
     */
    @Test
    public void t1RowKeyEncodingAndOrdering() {
        TemporalRowKeyCodec codec = new TemporalRowKeyCodec(8, WEEK);
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        byte[] prefix = codec.groupPrefix(GRAPH, LABEL, ENTITY, fk);

        long[] validFroms = {-30 * DAY, -1, 0, 1, 10 * DAY, 400 * DAY};
        List<byte[]> keys = new ArrayList<>();
        for (long vf : validFroms) {
            byte[] key = codec.historyRowKey(GRAPH, LABEL, ENTITY, fk, vf,
                                             "AAAAAAAAAAAAAAAAAAAA");
            Assert.assertTrue("row key must carry the group prefix",
                              TemporalRowKeyCodec.startsWith(key, prefix));
            keys.add(key);
        }
        for (int i = 1; i < keys.size(); i++) {
            Assert.assertTrue(
                    "byte order must follow valid_from order at index " + i,
                    TemporalRowKeyCodec.compare(keys.get(i - 1),
                                                keys.get(i)) < 0);
            Assert.assertTrue(
                    "bucket order must not contradict valid_from order",
                    codec.bucketOf(validFroms[i - 1]) <=
                    codec.bucketOf(validFroms[i]));
        }

        byte[] tieA = codec.historyRowKey(GRAPH, LABEL, ENTITY, fk, 0,
                                          "AAAAAAAAAAAAAAAAAAAA");
        byte[] tieB = codec.historyRowKey(GRAPH, LABEL, ENTITY, fk, 0,
                                          "BAAAAAAAAAAAAAAAAAAA");
        Assert.assertTrue("equal valid_from must order by tie_breaker",
                          TemporalRowKeyCodec.compare(tieA, tieB) < 0);
        Assert.assertEquals("all history keys must be fixed width",
                            tieA.length, tieB.length);

        // separator injection must not collapse two different components
        TemporalFactKey injected = fact("driver_1001", "order_88|X", "ACCEPTED");
        Assert.assertNotEquals(
                TemporalRowKeyCodec.hex(codec.groupPrefix(GRAPH, LABEL, ENTITY, fk)),
                TemporalRowKeyCodec.hex(codec.groupPrefix(GRAPH, LABEL, ENTITY,
                                                          injected)));

        log("T1", "prefix=" + TemporalRowKeyCodec.hex(prefix));
        log("T1", "rowKeyWidth=" + tieA.length +
                  " orderedValidFroms=" + Arrays.toString(validFroms));
    }

    // ------------------------------------------------------------------- T2

    /**
     * T2 tie_breaker is derived from the client mutation_id, design 2.1.
     * Deterministic across recomputation and replay, changes when mutation_id
     * changes, never generated by the server.
     */
    @Test
    public void t2TieBreakerDerivedFromMutationId() {
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        TemporalWrite.Request req = TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 0L, null, "p1", "mut-1");

        String a = TieBreakers.derive("mut-1", fk.canonicalBytes(),
                                      req.canonicalIntervalPayload());
        String b = TieBreakers.derive("mut-1", fk.canonicalBytes(),
                                      req.canonicalIntervalPayload());
        String c = TieBreakers.derive("mut-2", fk.canonicalBytes(),
                                      req.canonicalIntervalPayload());
        Assert.assertEquals("same mutation_id must derive the same tie_breaker",
                            a, b);
        Assert.assertNotEquals("different mutation_id must differ", a, c);
        Assert.assertEquals(TieBreakers.LENGTH, a.length());
        Assert.assertTrue("tie_breaker must be base32",
                          a.matches("[A-Z2-7]{20}"));

        // replay stability through the store
        TemporalBackendStoreSkeleton store = store();
        TemporalWrite.Result first = store.write(req);
        TemporalWrite.Result replay = store.write(TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 0L, null, "p1", "mut-1"));
        Assert.assertEquals(TemporalWrite.Status.APPLIED, first.status());
        Assert.assertEquals(TemporalWrite.Status.IDEMPOTENT_NOOP,
                            replay.status());
        Assert.assertEquals(first.tieBreaker(), replay.tieBreaker());
        Assert.assertArrayEquals(first.historyRowKey(), replay.historyRowKey());
        Assert.assertEquals(1, store.rowsOf(group(fk)));

        log("T2", "tieBreaker=" + a + " replayStable=true rows=" +
                  store.rowsOf(group(fk)));
    }

    // ------------------------------------------------------------------- T3

    /**
     * T3 fact_key_hash collision is not identity, design section 2.1.
     * Two different fact keys forced into the same row key prefix must remain
     * independent fact sequences: no false conflict and no row loss.
     */
    @Test
    public void t3FactKeyHashCollisionIsNotIdentity() {
        TemporalRowKeyCodec narrow = new TemporalRowKeyCodec(1, WEEK);
        TemporalFactKey left = null;
        TemporalFactKey right = null;
        Map<String, TemporalFactKey> seen = new HashMap<>();
        for (int i = 0; i < 4096 && right == null; i++) {
            TemporalFactKey candidate = fact("driver_1001", "order_" + i,
                                             "ACCEPTED");
            String hash = TemporalRowKeyCodec.hex(narrow.factKeyHash(candidate));
            TemporalFactKey prev = seen.get(hash);
            if (prev != null) {
                left = prev;
                right = candidate;
            } else {
                seen.put(hash, candidate);
            }
        }
        Assert.assertNotNull("failed to construct a fact_key_hash collision",
                             right);
        Assert.assertNotEquals(left, right);
        Assert.assertEquals(
                TemporalRowKeyCodec.hex(narrow.factKeyHash(left)),
                TemporalRowKeyCodec.hex(narrow.factKeyHash(right)));
        Assert.assertArrayEquals(
                narrow.groupPrefix(GRAPH, LABEL, ENTITY, left),
                narrow.groupPrefix(GRAPH, LABEL, ENTITY, right));

        TemporalBackendStoreSkeleton store =
                store(narrow, 1000, 1 << 20);
        store.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, left,
                                                 0L, null, "left-v1", "m-l1"));
        // identical valid-time on the colliding fact key must NOT conflict
        store.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, right,
                                                 0L, null, "right-v1", "m-r1"));

        Assert.assertEquals(2, store.factKeysUnderPrefix(group(left)));
        List<TemporalWrite.HistoryRow> leftRows = store.historyOf(group(left));
        List<TemporalWrite.HistoryRow> rightRows = store.historyOf(group(right));
        Assert.assertEquals(1, leftRows.size());
        Assert.assertEquals(1, rightRows.size());
        Assert.assertEquals("left-v1", leftRows.get(0).payloadAsString());
        Assert.assertEquals("right-v1", rightRows.get(0).payloadAsString());
        Assert.assertEquals(left, leftRows.get(0).factKey());
        Assert.assertEquals(right, rightRows.get(0).factKey());

        log("T3", "collidingHash=" +
                  TemporalRowKeyCodec.hex(narrow.factKeyHash(left)) +
                  " factKeysUnderPrefix=" + store.factKeysUnderPrefix(group(left)) +
                  " leftRows=1 rightRows=1 falseConflict=false");
    }

    // ------------------------------------------------------------------- T4

    /**
     * T4 idempotency contract, Phase 0 section 5.3.
     * Same mutation_id with identical canonical content is a no-op, any
     * difference returns IDEMPOTENCY_CONFLICT.
     */
    @Test
    public void t4IdempotencyAndConflict() {
        TemporalBackendStoreSkeleton store = store();
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");

        TemporalWrite.Result first = store.write(TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 0L, null, "payload-A", "mut-idem"));
        TemporalWrite.Result second = store.write(TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 0L, null, "payload-A", "mut-idem"));
        Assert.assertEquals(TemporalWrite.Status.APPLIED, first.status());
        Assert.assertEquals(TemporalWrite.Status.IDEMPOTENT_NOOP,
                            second.status());
        Assert.assertEquals(first.revision(), second.revision());
        Assert.assertEquals(1, store.rowsOf(group(fk)));

        TemporalStoreException payloadDiff = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 0L, null, "payload-B",
                        "mut-idem")));
        Assert.assertEquals(TemporalErrorCode.IDEMPOTENCY_CONFLICT,
                            payloadDiff.code());

        TemporalStoreException intervalDiff = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 5L, null, "payload-A",
                        "mut-idem")));
        Assert.assertEquals(TemporalErrorCode.IDEMPOTENCY_CONFLICT,
                            intervalDiff.code());

        TemporalStoreException factDiff = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY,
                        fact("driver_1001", "order_99", "ACCEPTED"),
                        0L, null, "payload-A", "mut-idem")));
        Assert.assertEquals(TemporalErrorCode.IDEMPOTENCY_CONFLICT,
                            factDiff.code());

        log("T4", "noopRevision=" + second.revision() +
                  " rows=" + store.rowsOf(group(fk)) +
                  " conflicts=payload/interval/factKey");
    }

    // ------------------------------------------------------------------- T5

    /**
     * T5 half-open interval intersection, design section 3.1.
     * old_from < new_to and old_to > new_from rejects; touching boundaries do
     * not.
     */
    @Test
    public void t5HalfOpenIntervalConflict() {
        TemporalBackendStoreSkeleton store = store();
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        store.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                 10L, 20L, "v1", "m-base"));

        TemporalStoreException overlap = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 15L, 25L, "v2", "m-ov")));
        Assert.assertEquals(TemporalErrorCode.TEMPORAL_CONFLICT, overlap.code());
        Assert.assertTrue(overlap.diagnostics()
                                 .containsKey("existing_interval"));

        TemporalStoreException contained = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 12L, 18L, "v3", "m-in")));
        Assert.assertEquals(TemporalErrorCode.TEMPORAL_CONFLICT,
                            contained.code());

        // adjacent half-open intervals are legal on both sides
        TemporalWrite.Result after = store.write(TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 20L, 30L, "v4", "m-after"));
        TemporalWrite.Result before = store.write(TemporalWrite.Request.append(
                GRAPH, LABEL, ENTITY, fk, 5L, 10L, "v5", "m-before"));
        Assert.assertEquals(TemporalWrite.Status.APPLIED, after.status());
        Assert.assertEquals(TemporalWrite.Status.APPLIED, before.status());
        Assert.assertEquals(3, store.historyOf(group(fk)).size());

        log("T5", "rejected=[15,25) and [12,18); accepted=[20,30) and [5,10); " +
                  "rows=" + store.historyOf(group(fk)).size());
    }

    // ------------------------------------------------------------------- T6

    /**
     * T6 cross-bucket open interval index, design section 3.1.
     * With the index the cold bucket write still sees the old open interval.
     * The negative control disables the index and proves it is load bearing.
     */
    @Test
    public void t6OpenIntervalIndexCoversBucketGap() {
        TemporalRowKeyCodec codec = new TemporalRowKeyCodec(8, WEEK);
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        long openFrom = 0L;
        long coldFrom = 30 * DAY;
        Assert.assertNotEquals("the two writes must land in different buckets",
                               codec.bucketOf(openFrom),
                               codec.bucketOf(coldFrom));

        TemporalBackendStoreSkeleton withIndex = store(codec, 1000, 1 << 20);
        withIndex.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                     openFrom, null, "open",
                                                     "m-open"));
        Assert.assertEquals(1, withIndex.openIntervalsOf(group(fk)).size());
        TemporalStoreException detected = Assert.assertThrows(
                TemporalStoreException.class,
                () -> withIndex.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, coldFrom, null, "cold",
                        "m-cold")));
        Assert.assertEquals(TemporalErrorCode.TEMPORAL_CONFLICT,
                            detected.code());

        // negative control: without the index the conflict is missed
        TemporalBackendStoreSkeleton noIndex = store(codec, 1000, 1 << 20);
        noIndex.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                   openFrom, null, "open",
                                                   "m-open"));
        noIndex.setOpenIntervalIndexEnabled(false);
        TemporalWrite.Result missed = noIndex.write(
                TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk, coldFrom,
                                             null, "cold", "m-cold"));
        Assert.assertEquals("negative control must show the miss",
                            TemporalWrite.Status.APPLIED, missed.status());

        // closing the old interval first makes the cold write legal
        TemporalBackendStoreSkeleton closedFirst = store(codec, 1000, 1 << 20);
        closedFirst.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                       openFrom, null, "open",
                                                       "m-open"));
        closedFirst.write(TemporalWrite.Request.close(GRAPH, LABEL, ENTITY, fk,
                                                      openFrom, 10 * DAY,
                                                      "m-close"));
        Assert.assertEquals(0, closedFirst.openIntervalsOf(group(fk)).size());
        TemporalWrite.Result legal = closedFirst.write(
                TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk, coldFrom,
                                             null, "cold", "m-cold"));
        Assert.assertEquals(TemporalWrite.Status.APPLIED, legal.status());

        log("T6", "bucketOpen=" + codec.bucketOf(openFrom) +
                  " bucketCold=" + codec.bucketOf(coldFrom) +
                  " withIndex=TEMPORAL_CONFLICT noIndex=MISSED(control) " +
                  "afterClose=APPLIED");
    }

    // ------------------------------------------------------------------- T7

    /**
     * T7 closed interval immutability, design section 3.2.
     * CLOSED to OPEN in place is forbidden; correction must use another flow.
     */
    @Test
    public void t7ClosedIntervalIsImmutable() {
        TemporalBackendStoreSkeleton store = store();
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        store.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                 100L, null, "v1", "m-open"));
        store.write(TemporalWrite.Request.close(GRAPH, LABEL, ENTITY, fk,
                                                100L, 200L, "m-close"));
        TemporalWrite.CurrentPointer current = store.currentOf(group(fk));
        Assert.assertEquals(TemporalWrite.State.CLOSED, current.state());
        Assert.assertEquals(Long.valueOf(200L), current.validTo());

        TemporalStoreException reopen = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.upsert(
                        GRAPH, LABEL, ENTITY, fk, 100L, null, "v2",
                        "m-reopen")));
        Assert.assertEquals(TemporalErrorCode.TEMPORAL_CLOSED_INTERVAL_CONFLICT,
                            reopen.code());
        Assert.assertEquals("CLOSED", reopen.diagnostics().get("state"));

        TemporalStoreException modify = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.upsert(
                        GRAPH, LABEL, ENTITY, fk, 100L, 300L, "v3",
                        "m-modify")));
        Assert.assertEquals(TemporalErrorCode.TEMPORAL_CLOSED_INTERVAL_CONFLICT,
                            modify.code());

        // the closed interval was not mutated by the rejected attempts
        TemporalWrite.HistoryRow row = store.historyOf(group(fk)).get(0);
        Assert.assertEquals(TemporalWrite.State.CLOSED, row.state());
        Assert.assertEquals(Long.valueOf(200L), row.validTo());
        Assert.assertEquals("v1", row.payloadAsString());

        log("T7", "closedInterval=[100,200) reopen=REJECTED modify=REJECTED " +
                  "payloadUnchanged=v1");
    }

    // ------------------------------------------------------------------- T8

    /**
     * T8 colocation placement and capacity, design section 3 and tracking item
     * B. A colocation group never crosses a Region; a multi-Region mutation is
     * rejected whole; hitting the single Region limit rejects the write and the
     * same mutation_id can not retry past it.
     */
    @Test
    public void t8ColocationPlacementAndCapacity() {
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        TemporalFactKey other = fact("driver_1001", "order_89", "ACCEPTED");
        ColocationPlacement placement = new ColocationPlacement(3);
        int region = placement.baseRegionOf(group(fk));
        for (ColocationPlacement.View view : placement.views()) {
            Assert.assertEquals("all views of one group share one Region",
                                region, placement.regionOf(group(fk), view));
        }
        Assert.assertEquals("placement must not depend on time or bucket",
                            region, placement.baseRegionOf(group(fk)));

        TemporalBackendStoreSkeleton store = new TemporalBackendStoreSkeleton(
                new TemporalRowKeyCodec(8, WEEK), placement, 2, 1 << 20);

        // (a) capacity: two rows fit, the third is rejected
        store.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                 0L, 10L, "v1", "m-1"));
        store.write(TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk,
                                                 10L, 20L, "v2", "m-2"));
        TemporalStoreException full = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 20L, 30L, "v3", "m-3")));
        Assert.assertEquals(
                TemporalErrorCode.TEMPORAL_COLOCATION_CAPACITY_EXCEEDED,
                full.code());
        Assert.assertEquals(2L, full.diagnostics().get("current_rows"));
        Assert.assertEquals(2L, full.diagnostics().get("max_rows"));
        Assert.assertTrue(full.diagnostics().containsKey("region_id"));
        Assert.assertTrue(full.diagnostics().containsKey("colocation_group"));

        // retrying the same mutation_id must not bypass the limit
        TemporalStoreException retry = Assert.assertThrows(
                TemporalStoreException.class,
                () -> store.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 20L, 30L, "v3", "m-3")));
        Assert.assertEquals(
                TemporalErrorCode.TEMPORAL_COLOCATION_CAPACITY_EXCEEDED,
                retry.code());
        Assert.assertEquals(2L, store.rowsOf(group(fk)));

        // a different fact key has its own capacity budget
        TemporalWrite.Result independent = store.write(
                TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, other,
                                             0L, 10L, "o1", "m-o1"));
        Assert.assertEquals(TemporalWrite.Status.APPLIED, independent.status());

        // (b) a mutation touching two Regions is rejected whole
        ColocationPlacement split = new ColocationPlacement(3);
        int base = split.baseRegionOf(group(fk));
        split.injectSplitPlacement(group(fk), ColocationPlacement.View.CURRENT,
                                   (base + 1) % 3);
        TemporalBackendStoreSkeleton splitStore =
                new TemporalBackendStoreSkeleton(
                        new TemporalRowKeyCodec(8, WEEK), split, 100, 1 << 20);
        TemporalStoreException crossRegion = Assert.assertThrows(
                TemporalStoreException.class,
                () -> splitStore.write(TemporalWrite.Request.append(
                        GRAPH, LABEL, ENTITY, fk, 0L, 10L, "v1", "m-x")));
        Assert.assertEquals(TemporalErrorCode.TEMPORAL_CROSS_REGION_UNSUPPORTED,
                            crossRegion.code());
        Assert.assertEquals(0L, splitStore.rowsOf(group(fk)));
        Assert.assertNull("no partial commit is allowed",
                          splitStore.currentOf(group(fk)));

        log("T8", "region=" + region + " capacityRejected=true retryBlocked=true " +
                  "crossRegionRejected=true partialCommit=false");
    }

    // ------------------------------------------------------------------- A2

    /**
     * Guard invariant: history row, current pointer and open interval index of
     * one mutation are applied at one committed revision, design section 3.
     */
    @Test
    public void a2SingleRevisionAtomicApply() {
        TemporalBackendStoreSkeleton store = store();
        TemporalFactKey fk = fact("driver_1001", "order_88", "ACCEPTED");
        TemporalWrite.Result result = store.write(
                TemporalWrite.Request.append(GRAPH, LABEL, ENTITY, fk, 0L, null,
                                             "v1", "m-atomic"));
        long rev = result.revision();
        Assert.assertEquals(rev, result.currentRevision());
        Assert.assertEquals(rev, result.openIndexRevision());
        Assert.assertEquals(rev, store.currentOf(group(fk)).revision());
        Assert.assertEquals(rev, store.historyOf(group(fk)).get(0).revision());
        Assert.assertEquals(rev,
                            store.openIntervalsOf(group(fk)).get(0).revision());
        log("A2", "revision=" + rev +
                  " views=history/current/openIndex all equal");
    }
}
