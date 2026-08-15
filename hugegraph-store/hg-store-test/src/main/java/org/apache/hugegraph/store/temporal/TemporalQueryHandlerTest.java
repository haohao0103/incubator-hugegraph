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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Store handler-level tests for the temporal read path. This proves the
 * fact-scoped {@code as_of}/{@code between}/{@code overlap} semantics and the
 * append -&gt; as_of closed loop at the handler level; it is NOT evidence of a
 * real HStore/PD/Raft cluster result.
 */
public class TemporalQueryHandlerTest {

    // --------------------------------------------------------------- pure reads

    @Test
    public void shouldAsOfReturnContainingInterval() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 41L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        List<TemporalIntervalRow> rows = query.asOf("g", factKey, 150L);

        assertEquals(1, rows.size());
        assertEquals(100L, rows.get(0).validFrom());
        assertEquals(Long.valueOf(200L), rows.get(0).validTo());
        assertEquals(41L, rows.get(0).committedRevision());
    }

    @Test
    public void shouldAsOfReturnEmptyInGap() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        // Each scanPrefix must return a fresh iterator: ListScanIterator is
        // single-use, and asOf is invoked three times below.
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 41L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        assertTrue(query.asOf("g", factKey, 250L).isEmpty());
        // valid_from inclusive bound: time == valid_from belongs to the interval
        assertEquals(1, query.asOf("g", factKey, 100L).size());
        // valid_to exclusive bound: time == valid_to is outside
        assertTrue(query.asOf("g", factKey, 200L).isEmpty());
    }

    @Test
    public void shouldAsOfContainOpenIntervalLateTime() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L,
                                              TemporalIntervalCodec.OPEN_VALID_TO, 41L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        List<TemporalIntervalRow> rows = query.asOf("g", factKey, Long.MAX_VALUE - 1);

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).open());
        assertEquals(41L, rows.get(0).committedRevision());
    }

    @Test
    public void shouldBetweenReturnOnlyOverlapping() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 1L),
                                       marker(factKey, 300L, 400L, 2L),
                                       marker(factKey, 500L, 600L, 3L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        // [150,350) overlaps [100,200) and [300,400), but not [500,600)
        List<TemporalIntervalRow> rows = query.between("g", factKey, 150L, 350L);

        assertEquals(2, rows.size());
        assertEquals(100L, rows.get(0).validFrom());
        assertEquals(300L, rows.get(1).validFrom());
    }

    @Test
    public void shouldAdjacentIntervalsNotOverlap() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 1L),
                                       marker(factKey, 200L, 300L, 2L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        // [200,300) query: [100,200) is adjacent (touching) not overlapping
        List<TemporalIntervalRow> rows = query.overlap("g", factKey, 200L, 300L);

        assertEquals(1, rows.size());
        assertEquals(200L, rows.get(0).validFrom());
    }

    @Test
    public void shouldFilterHashCollisionLongerFactKey() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        byte[] longer = bytes("fact-extra");
        // "fact-extra" shares the "fact" prefix; its marker key is longer than
        // factKey.length + 16 and must never be folded into the fact sequence.
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 1L),
                                       marker(longer, 100L, 200L, 2L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        List<TemporalIntervalRow> rows = query.asOf("g", factKey, 150L);

        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).committedRevision());
    }

    @Test
    public void shouldEnforceScanLimit() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        // No interval contains 200; the bucket scan must read all three rows
        // before giving up, so it trips the row budget (maxScanRows=2).
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 110L, 1L),
                                       marker(factKey, 110L, 120L, 2L),
                                       marker(factKey, 120L, 130L, 3L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business, 2L);
        try {
            query.asOf("g", factKey, 200L);
            fail("expected TEMPORAL_QUERY_LIMIT_EXCEEDED");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_QUERY_LIMIT_EXCEEDED, e.getCode());
        }
    }

    @Test
    public void shouldOrderResultsByValidFrom() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        // Deliberately out of key order: the read path must still return
        // valid_from ascending.
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 300L, 400L, 3L),
                                       marker(factKey, 100L, 200L, 1L),
                                       marker(factKey, 200L, 300L, 2L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        List<TemporalIntervalRow> rows = query.between("g", factKey, 0L, 600L);

        assertEquals(3, rows.size());
        assertEquals(100L, rows.get(0).validFrom());
        assertEquals(200L, rows.get(1).validFrom());
        assertEquals(300L, rows.get(2).validFrom());
    }

    @Test
    public void shouldSeekBucketNotScanFullFactPrefix() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 1L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        List<TemporalIntervalRow> rows = query.asOf("g", factKey, 150L);
        assertEquals(1, rows.size());

        org.mockito.ArgumentCaptor<byte[]> prefixes =
                org.mockito.ArgumentCaptor.forClass(byte[].class);
        org.mockito.Mockito.verify(business, org.mockito.Mockito.atLeast(2))
                            .scanPrefix(anyString(), anyInt(), anyString(),
                                        prefixes.capture());
        List<byte[]> scanned = prefixes.getAllValues();
        // Every scanned prefix must be fact_key + version + a bucket suffix
        // (never the bare fact_key), proving the read seeks buckets, not the
        // whole fact.
        for (byte[] prefix : scanned) {
            assertEquals(factKey.length + 1 + TemporalIntervalCodec.BUCKET_BYTES,
                         prefix.length);
        }
        // The second scan targets the bucket containing 150 (bucket 0).
        assertArrayEquals(TemporalIntervalCodec.bucketPrefix(factKey, 0L),
                          scanned.get(1));
    }

    @Test
    public void shouldWalkBackBucketForClosedIntervalSpanning() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        long day = 24L * 3600 * 1000;
        long bucket = 7 * day;
        // [100, bucket+1000) starts in bucket 0 but spans into bucket 1.
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, bucket + 1000L, 1L)));

        TemporalQueryHandler query = new TemporalQueryHandler(business);
        List<TemporalIntervalRow> rows = query.asOf("g", factKey, bucket + 500L);

        assertEquals(1, rows.size());
        assertEquals(100L, rows.get(0).validFrom());
        assertEquals(Long.valueOf(bucket + 1000L), rows.get(0).validTo());
    }

    @Test
    public void shouldRejectRangeSpanningTooManyBuckets() {
        BusinessHandler business = mock(BusinessHandler.class);
        byte[] factKey = bytes("fact");
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenAnswer(scanAnswer(marker(factKey, 100L, 200L, 1L)));

        // maxBuckets=2; a range spanning 10 buckets must be rejected up front.
        TemporalQueryHandler query = new TemporalQueryHandler(business, 1000L, 2L);
        try {
            query.between("g", factKey, 0L, 10 * TemporalIntervalCodec.BUCKET_WIDTH_MILLIS);
            fail("expected TEMPORAL_QUERY_LIMIT_EXCEEDED");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_QUERY_LIMIT_EXCEEDED, e.getCode());
        }
    }

    // ----------------------------------------------------- append -> as_of loop

    @Test
    public void shouldRoundTripAppendThenAsOf() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);
        TemporalQueryHandler reader = new TemporalQueryHandler(business);

        byte[] factKey = bytes("fact");
        TemporalMutationBundle bundle = bundle(factKey, "m1", 100L, 200L);
        assertEquals(true, writer.invoke(7, request(bundle), null, 41L));

        List<TemporalIntervalRow> rows = reader.asOf("g", factKey, 150L);
        assertEquals(1, rows.size());
        assertEquals(100L, rows.get(0).validFrom());
        assertEquals(Long.valueOf(200L), rows.get(0).validTo());
        assertEquals(41L, rows.get(0).committedRevision());
    }

    @Test
    public void shouldRoundTripOpenAppendThenAsOf() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);
        TemporalQueryHandler reader = new TemporalQueryHandler(business);

        byte[] factKey = bytes("fact");
        TemporalMutationBundle bundle = openBundle(factKey, "m1", 100L);
        assertEquals(true, writer.invoke(7, request(bundle), null, 41L));

        List<TemporalIntervalRow> rows = reader.asOf("g", factKey, Long.MAX_VALUE - 1);
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).open());
    }

    // ------------------------------------------------- append -> close/delete loop

    @Test
    public void shouldRoundTripAppendCloseThenAsOf() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);
        TemporalQueryHandler reader = new TemporalQueryHandler(business);

        byte[] factKey = bytes("fact");
        assertEquals(true, writer.invoke(7, request(openBundle(factKey, "m1", 100L)),
                                        null, 41L));
        assertEquals(true, writer.invoke(7, request(closeBundle(factKey, "m2", 100L, 200L)),
                                        null, 42L));

        // The interval is now [100, 200): still visible at 150, gone at 250.
        List<TemporalIntervalRow> rows = reader.asOf("g", factKey, 150L);
        assertEquals(1, rows.size());
        assertEquals(100L, rows.get(0).validFrom());
        assertEquals(Long.valueOf(200L), rows.get(0).validTo());
        assertEquals(42L, rows.get(0).committedRevision());
        assertTrue(reader.asOf("g", factKey, 250L).isEmpty());
    }

    @Test
    public void shouldRoundTripAppendDeleteThenAsOf() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);
        TemporalQueryHandler reader = new TemporalQueryHandler(business);

        byte[] factKey = bytes("fact");
        assertEquals(true, writer.invoke(7, request(bundle(factKey, "m1", 100L, 200L)),
                                        null, 41L));
        assertEquals(true, writer.invoke(7, request(deleteBundle(factKey, "m2", 100L)),
                                        null, 42L));

        // A deleted interval is not part of the valid timeline.
        assertTrue(reader.asOf("g", factKey, 150L).isEmpty());
    }

    @Test
    public void shouldCloseMissingIntervalConflict() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);

        byte[] factKey = bytes("fact");
        try {
            writer.invoke(7, request(closeBundle(factKey, "m1", 100L, 200L)), null, 41L);
            fail("expected TEMPORAL_CONFLICT");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_CONFLICT, e.getCode());
        }
    }

    @Test
    public void shouldCloseAlreadyClosedIntervalConflict() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);

        byte[] factKey = bytes("fact");
        // Append a CLOSED interval [100,200), then close it at a different time.
        assertEquals(true, writer.invoke(7, request(bundle(factKey, "m1", 100L, 200L)),
                                        null, 41L));
        try {
            writer.invoke(7, request(closeBundle(factKey, "m2", 100L, 300L)), null, 42L);
            fail("expected TEMPORAL_CLOSED_INTERVAL_CONFLICT");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_CLOSED_INTERVAL_CONFLICT, e.getCode());
        }
    }

    @Test
    public void shouldDeleteMissingIntervalConflict() throws Exception {
        StatefulStore store = new StatefulStore();
        BusinessHandler business = store.businessHandler();
        TemporalMutationHandler writer = new TemporalMutationHandler(business);

        byte[] factKey = bytes("fact");
        try {
            writer.invoke(7, request(deleteBundle(factKey, "m1", 100L)), null, 41L);
            fail("expected TEMPORAL_CONFLICT");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_CONFLICT, e.getCode());
        }
    }

    // ---------------------------------------------------------------- helpers

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static RocksDBSession.BackendColumn marker(byte[] factKey, long from,
                                                       long to, long revision) {
        return RocksDBSession.BackendColumn.of(
                TemporalIntervalCodec.intervalKey(factKey, from, to),
                TemporalIntervalCodec.value(new byte[]{1}, revision));
    }

    /**
     * Prefix-filtering scan answer: the Phase 4 read path issues several
     * bucket-prefixed {@code scanPrefix} calls, so a mock must return only the
     * columns whose key starts with the requested prefix (like a real Store).
     */
    private static org.mockito.stubbing.Answer<ScanIterator> scanAnswer(
            RocksDBSession.BackendColumn... columns) {
        return invocation -> {
            byte[] prefix = invocation.getArgument(3);
            List<RocksDBSession.BackendColumn> matching = new ArrayList<>();
            for (RocksDBSession.BackendColumn column : columns) {
                if (startsWith(column.name, prefix)) {
                    matching.add(column);
                }
            }
            return new ListScanIterator(matching);
        };
    }

    private static TemporalMutationBundle bundle(byte[] factKey, String id,
                                                 long from, long to) {
        List<TemporalMutationBundle.ViewMutation> views = Arrays.asList(
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_HISTORY_TABLE, bytes("h"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_CURRENT_TABLE, bytes("c"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE, bytes("o"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_INDEX_TABLE, bytes("i"), bytes("v")));
        return new TemporalMutationBundle("g", "label", "entity", factKey, id, 1,
                                          from, to, false, bytes("payload"), views);
    }

    private static TemporalMutationBundle openBundle(byte[] factKey, String id,
                                                     long from) {
        List<TemporalMutationBundle.ViewMutation> views = Arrays.asList(
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_HISTORY_TABLE, bytes("h"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_CURRENT_TABLE, bytes("c"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE, bytes("o"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_INDEX_TABLE, bytes("i"), bytes("v")));
        return new TemporalMutationBundle("g", "label", "entity", factKey, id, 1,
                                          from, 0L, true, bytes("payload"), views);
    }

    private static TemporalMutationBundle closeBundle(byte[] factKey, String id,
                                                      long from, long closeAt) {
        return new TemporalMutationBundle(TemporalMutationBundle.Operation.CLOSE,
                                          "g", "label", "entity", factKey, id, 1,
                                          from, closeAt, false, new byte[0],
                                          Collections.emptyList());
    }

    private static TemporalMutationBundle deleteBundle(byte[] factKey, String id,
                                                       long from) {
        return new TemporalMutationBundle(TemporalMutationBundle.Operation.DELETE,
                                          "g", "label", "entity", factKey, id, 1,
                                          from, 0L, true, new byte[0],
                                          Collections.emptyList());
    }

    private static byte[] request(TemporalMutationBundle bundle) throws Exception {
        byte[] encoded = TemporalMutationBundleCodec.encode(bundle);
        byte[] request = new byte[encoded.length + 1];
        request[0] = TemporalMutationHandler.TEMPORAL_MUTATION;
        System.arraycopy(encoded, 0, request, 1, encoded.length);
        return request;
    }

    private static final class ListScanIterator implements ScanIterator {

        private final Iterator<RocksDBSession.BackendColumn> iterator;

        ListScanIterator(List<RocksDBSession.BackendColumn> columns) {
            this.iterator = columns.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public boolean isValid() {
            return this.iterator.hasNext();
        }

        @Override
        public <T> T next() {
            return (T) this.iterator.next();
        }

        @Override
        public void close() {
        }
    }

    /**
     * Minimal stateful mock of the {@link BusinessHandler} surface used by both
     * the write handler ({@code doGet}/{@code scanPrefix}/{@code txBuilder}/
     * {@code getSession}) and the read handler ({@code scanPrefix}). Writes
     * land in an in-memory sorted map so the append -&gt; as_of loop is a real
     * round trip through the production handlers.
     */
    private static final class StatefulStore {

        private final Map<KeyTuple, byte[]> store = new ConcurrentSkipListMap<>();
        private final List<PendingWrite> pending = new ArrayList<>();
        private final BusinessHandler businessHandler = mock(BusinessHandler.class);

        StatefulStore() {
            RocksDBSession session = mock(RocksDBSession.class);
            when(session.getDbPath()).thenReturn("/tmp/temporal-query-test");
            when(this.businessHandler.getSession(anyInt())).thenReturn(session);

            when(this.businessHandler.doGet(anyString(), anyInt(), anyString(), any()))
                    .thenAnswer(invocation -> this.store.get(
                            new KeyTuple(invocation.getArgument(2), invocation.getArgument(3))));

            when(this.businessHandler.scanPrefix(anyString(), anyInt(), anyString(), any()))
                    .thenAnswer(invocation -> {
                        String table = invocation.getArgument(2);
                        byte[] prefix = invocation.getArgument(3);
                        List<RocksDBSession.BackendColumn> columns = new ArrayList<>();
                        for (Map.Entry<KeyTuple, byte[]> entry : this.store.entrySet()) {
                            if (entry.getKey().table.equals(table) &&
                                startsWith(entry.getKey().key, prefix)) {
                                columns.add(RocksDBSession.BackendColumn.of(
                                        entry.getKey().key, entry.getValue()));
                            }
                        }
                        return new ListScanIterator(columns);
                    });

            BusinessHandler.TxBuilder builder = mock(BusinessHandler.TxBuilder.class);
            when(builder.put(anyInt(), anyString(), any(), any()))
                    .thenAnswer(invocation -> {
                        this.pending.add(new PendingWrite(invocation.getArgument(1),
                                                          invocation.getArgument(2),
                                                          invocation.getArgument(3)));
                        return builder;
                    });
            when(builder.del(anyInt(), anyString(), any()))
                    .thenAnswer(invocation -> {
                        this.pending.add(new PendingWrite(invocation.getArgument(1),
                                                          invocation.getArgument(2),
                                                          null));
                        return builder;
                    });
            BusinessHandler.Tx tx = mock(BusinessHandler.Tx.class);
            doAnswer(invocation -> {
                for (PendingWrite write : this.pending) {
                    KeyTuple key = new KeyTuple(write.table, write.key);
                    if (write.value == null) {
                        this.store.remove(key);
                    } else {
                        this.store.put(key, write.value);
                    }
                }
                this.pending.clear();
                return null;
            }).when(tx).commit();
            doNothing().when(tx).rollback();
            when(builder.build()).thenReturn(tx);
            when(this.businessHandler.txBuilder(anyString(), anyInt())).thenReturn(builder);
        }

        BusinessHandler businessHandler() {
            return this.businessHandler;
        }
    }

    private static final class PendingWrite {

        final String table;
        final byte[] key;
        /** {@code null} means delete. */
        final byte[] value;

        PendingWrite(String table, byte[] key, byte[] value) {
            this.table = table;
            this.key = key;
            this.value = value;
        }
    }

    private static final class KeyTuple implements Comparable<KeyTuple> {

        final String table;
        final byte[] key;

        KeyTuple(String table, byte[] key) {
            this.table = table;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof KeyTuple)) {
                return false;
            }
            KeyTuple other = (KeyTuple) o;
            return this.table.equals(other.table) && Arrays.equals(this.key, other.key);
        }

        @Override
        public int hashCode() {
            return 31 * this.table.hashCode() + Arrays.hashCode(this.key);
        }

        @Override
        public int compareTo(KeyTuple other) {
            int c = this.table.compareTo(other.table);
            if (c != 0) {
                return c;
            }
            return TemporalIntervalCodecCompare.compare(this.key, other.key);
        }
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /** Unsigned lexicographic byte comparison mirroring the Store row order. */
    private static final class TemporalIntervalCodecCompare {

        static int compare(byte[] a, byte[] b) {
            int len = Math.min(a.length, b.length);
            for (int i = 0; i < len; i++) {
                int x = a[i] & 0xFF;
                int y = b[i] & 0xFF;
                if (x != y) {
                    return x < y ? -1 : 1;
                }
            }
            return Integer.compare(a.length, b.length);
        }
    }
}
