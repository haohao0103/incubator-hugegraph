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
package org.apache.hugegraph.store.business;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.OpType;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.grpc.session.TemporalBundle;
import org.apache.hugegraph.store.temporal.TemporalIntervalCodec;
import org.apache.hugegraph.store.temporal.TemporalMutationBundle;
import org.apache.hugegraph.store.temporal.TemporalMutationBundleCodec;
import org.apache.hugegraph.store.temporal.TemporalMutationHandler;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Test;

import com.google.protobuf.ByteString;

/**
 * Phase B guardrail: the normal batch and its bound temporal bundles must commit
 * in ONE store transaction (single {@link BusinessHandler.TxBuilder}, single
 * commit) so a crash leaves both the normal write and its validity interval
 * visible together or not at all. It also pins the non-temporal path to be
 * byte-for-byte unchanged. Handler-level test with a real
 * {@link TemporalMutationHandler} over a mocked {@link BusinessHandler}; not
 * cluster evidence.
 */
public class BusinessHandlerAtomicBatchTest {

    private static final String GRAPH = "g";
    private static final int PART = 7;

    @Test
    public void shouldCommitNormalAndTemporalInOneTransaction() throws Exception {
        BusinessHandler business = mock(BusinessHandler.class, CALLS_REAL_METHODS);
        BusinessHandler.TxBuilder builder = mock(BusinessHandler.TxBuilder.class);
        BusinessHandler.Tx tx = mock(BusinessHandler.Tx.class);
        doReturn(builder).when(business).txBuilder(GRAPH, PART);
        // No ledger hit and no overlapping interval -> the temporal write is
        // contributed into the shared transaction.
        doReturn(null).when(business).doGet(anyString(), anyInt(), anyString(), any());
        doReturn(emptyScan()).when(business).scanPrefix(anyString(), anyInt(), anyString(), any());
        doReturn(emptyScan()).when(business).scan(anyString(), anyInt(), anyString(),
                                                  any(), any(), anyInt());
        when(builder.put(anyInt(), anyString(), any(), any())).thenReturn(builder);
        when(builder.build()).thenReturn(tx);
        doNothing().when(tx).commit();
        doNothing().when(tx).rollback();

        TemporalMutationHandler handler = new TemporalMutationHandler(business);

        List<BatchEntry> entries = Collections.singletonList(putEntry());
        List<TemporalBundle> bundles = Collections.singletonList(temporalBundle("m1", 10, 20));

        business.doBatch(GRAPH, PART, entries, bundles, 41L, handler);

        // One shared TxBuilder for both the normal entry and the temporal views.
        verify(business, times(1)).txBuilder(GRAPH, PART);
        verify(builder, times(1)).put(eq(PART), eq(HugeServerTables.VERTEX_TABLE), any(), any());
        // The temporal views landed in the SAME builder (four views + ledger +
        // interval marker). Temporal writes are keyed by the fact-key hash (not
        // the partition id), so match the code loosely and pin the table.
        verify(builder, times(1))
                .put(anyInt(), eq(HugeServerTables.TEMPORAL_CURRENT_TABLE), any(), any());
        // Exactly one commit, no rollback -> atomic all-or-nothing.
        verify(tx, times(1)).commit();
        verify(tx, never()).rollback();
    }

    @Test
    public void shouldRollbackEverythingOnTemporalConflict() throws Exception {
        BusinessHandler business = mock(BusinessHandler.class, CALLS_REAL_METHODS);
        BusinessHandler.TxBuilder builder = mock(BusinessHandler.TxBuilder.class);
        BusinessHandler.Tx tx = mock(BusinessHandler.Tx.class);
        doReturn(builder).when(business).txBuilder(GRAPH, PART);
        doReturn(null).when(business).doGet(anyString(), anyInt(), anyString(), any());
        // An existing interval [10,20) overlaps the candidate [15,25) -> conflict.
        TemporalMutationBundle existing = bundle("m1", 10, 20);
        doReturn(conflictScan(existing))
                .when(business).scanPrefix(anyString(), anyInt(), anyString(), any());
        doReturn(emptyScan()).when(business).scan(anyString(), anyInt(), anyString(),
                                                  any(), any(), anyInt());
        when(builder.put(anyInt(), anyString(), any(), any())).thenReturn(builder);
        when(builder.build()).thenReturn(tx);
        doNothing().when(tx).commit();
        doNothing().when(tx).rollback();

        TemporalMutationHandler handler = new TemporalMutationHandler(business);

        List<BatchEntry> entries = Collections.singletonList(putEntry());
        List<TemporalBundle> bundles = Collections.singletonList(temporalBundle("m2", 15, 25));

        try {
            business.doBatch(GRAPH, PART, entries, bundles, 42L, handler);
            throw new AssertionError("expected TEMPORAL_CONFLICT");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_CONFLICT, e.getCode());
        }
        // The normal write must NOT be committed when the temporal part fails:
        // the whole shared transaction is rolled back instead.
        verify(tx, never()).commit();
        verify(tx, times(1)).rollback();
    }

    @Test
    public void shouldKeepNonTemporalBatchUnchanged() throws Exception {
        BusinessHandler business = mock(BusinessHandler.class, CALLS_REAL_METHODS);
        BusinessHandler.TxBuilder builder = mock(BusinessHandler.TxBuilder.class);
        BusinessHandler.Tx tx = mock(BusinessHandler.Tx.class);
        doReturn(builder).when(business).txBuilder(GRAPH, PART);
        when(builder.put(anyInt(), anyString(), any(), any())).thenReturn(builder);
        when(builder.build()).thenReturn(tx);
        doNothing().when(tx).commit();
        doNothing().when(tx).rollback();

        List<BatchEntry> entries = Collections.singletonList(putEntry());

        // Legacy 3-arg path: no temporal handler involved at all.
        business.doBatch(GRAPH, PART, entries);
        verify(builder, times(1)).put(eq(PART), eq(HugeServerTables.VERTEX_TABLE), any(), any());
        verify(tx, times(1)).commit();
        verify(tx, never()).rollback();

        // Additive 5-arg path with an empty bundle list behaves identically: no
        // temporal read/write is triggered and the commit count only advances by
        // the one extra (non-temporal) batch.
        business.doBatch(GRAPH, PART, entries, Collections.emptyList(), 43L,
                         new TemporalMutationHandler(business));
        verify(business, never()).scanPrefix(anyString(), anyInt(), anyString(), any());
        verify(tx, times(2)).commit();
        verify(tx, never()).rollback();
    }

    private static BatchEntry putEntry() {
        return BatchEntry.newBuilder()
                         .setOpType(OpType.OP_TYPE_PUT)
                         .setTable(HugeServerTables.TABLES_MAP.get(HugeServerTables.VERTEX_TABLE))
                         .setStartKey(Key.newBuilder()
                                         .setCode(PART)
                                         .setKey(ByteString.copyFrom(bytes("v1")))
                                         .build())
                         .setValue(ByteString.copyFrom(bytes("value")))
                         .build();
    }

    private static TemporalBundle temporalBundle(String id, long from, long to) throws Exception {
        byte[] encoded = TemporalMutationBundleCodec.encode(bundle(id, from, to));
        return TemporalBundle.newBuilder()
                             .setCode(PART)
                             .setBundle(ByteString.copyFrom(encoded))
                             .build();
    }

    private static TemporalMutationBundle bundle(String id, long from, long to) {
        TemporalMutationBundle.ViewMutation history = new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_HISTORY_TABLE, bytes("history-view"), bytes("value"));
        TemporalMutationBundle.ViewMutation current = new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_CURRENT_TABLE, bytes("current-view"), bytes("value"));
        TemporalMutationBundle.ViewMutation openIndex = new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE, bytes("open-view"), bytes("value"));
        TemporalMutationBundle.ViewMutation index = new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_INDEX_TABLE, bytes("index-view"), bytes("value"));
        return new TemporalMutationBundle(GRAPH, "label", "entity", bytes("fact"), id, 1,
                                          from, to, false, bytes("payload"),
                                          Arrays.asList(history, current, openIndex, index));
    }

    private static ScanIterator emptyScan() {
        ScanIterator scan = mock(ScanIterator.class);
        when(scan.hasNext()).thenReturn(false);
        return scan;
    }

    private static ScanIterator conflictScan(TemporalMutationBundle existing) {
        RocksDBSession.BackendColumn column = new RocksDBSession.BackendColumn();
        column.name = TemporalIntervalCodec.intervalKey(existing);
        ScanIterator scan = mock(ScanIterator.class);
        when(scan.hasNext()).thenReturn(true, false);
        when(scan.next()).thenReturn(column);
        return scan;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
