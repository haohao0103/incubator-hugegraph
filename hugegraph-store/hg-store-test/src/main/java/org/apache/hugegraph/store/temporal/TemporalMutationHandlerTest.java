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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Store handler-level test; it is not evidence of a 3+3+3 cluster TOCTOU result. */
public class TemporalMutationHandlerTest {

    @Test
    public void shouldApplyConflictAndReplayIdempotently() throws Exception {
        BusinessHandler business = mock(BusinessHandler.class);
        BusinessHandler.TxBuilder builder = mock(BusinessHandler.TxBuilder.class);
        BusinessHandler.Tx tx = mock(BusinessHandler.Tx.class);
        TemporalMutationHandler handler = new TemporalMutationHandler(business);
        // The apply path borrows a RocksDB session for its instrumentation line;
        // return a stub so the handler can read its path without a real DB.
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getDbPath()).thenReturn("/tmp/temporal-handler-test");
        when(business.getSession(anyInt())).thenReturn(session);
        when(business.doGet(anyString(), anyInt(), anyString(), any())).thenReturn(null);
        when(business.txBuilder("g", 7)).thenReturn(builder);
        when(builder.put(anyInt(), anyString(), any(), any())).thenReturn(builder);
        when(builder.build()).thenReturn(tx);
        doNothing().when(tx).commit();
        doNothing().when(tx).rollback();
        ScanIterator initialScan = emptyScan();
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenReturn(initialScan);

        TemporalMutationBundle first = bundle("m1", 10, 20);
        assertEquals(true, handler.invoke(7, request(first), null, 41L));
        verify(tx).commit();
        verifyRevisionWrites(builder, 41L);

        when(business.doGet(anyString(), anyInt(), anyString(), any())).thenReturn(new byte[0]);
        handler.invoke(7, request(first), null, 42L);
        verify(tx, times(1)).commit();

        when(business.doGet(anyString(), anyInt(), anyString(), any())).thenReturn(null);
        RocksDBSession.BackendColumn column = new RocksDBSession.BackendColumn();
        column.name = intervalKey(first);
        ScanIterator existing = mock(ScanIterator.class);
        when(existing.hasNext()).thenReturn(true, false);
        when(existing.next()).thenReturn(column);
        when(business.scanPrefix(anyString(), anyInt(), anyString(), any()))
                .thenReturn(existing);
        try {
            handler.invoke(7, request(bundle("m2", 15, 25)), null, 43L);
            throw new AssertionError("expected TEMPORAL_CONFLICT");
        } catch (HgStoreException e) {
            assertEquals(HgStoreException.EC_TEMPORAL_CONFLICT, e.getCode());
        }
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
        return new TemporalMutationBundle("g", "label", "entity", bytes("fact"), id, 1,
                                          from, to, false, bytes("payload"),
                                          Arrays.asList(history, current, openIndex, index));
    }

    private static byte[] request(TemporalMutationBundle bundle) throws Exception {
        byte[] encoded = TemporalMutationBundleCodec.encode(bundle);
        byte[] request = new byte[encoded.length + 1];
        request[0] = TemporalMutationHandler.TEMPORAL_MUTATION;
        System.arraycopy(encoded, 0, request, 1, encoded.length);
        return request;
    }

    private static void verifyRevisionWrites(BusinessHandler.TxBuilder builder,
                                              long revision) {
        org.mockito.ArgumentCaptor<String> tables =
                org.mockito.ArgumentCaptor.forClass(String.class);
        org.mockito.ArgumentCaptor<byte[]> values =
                org.mockito.ArgumentCaptor.forClass(byte[].class);
        verify(builder, times(6)).put(anyInt(), tables.capture(), any(), values.capture());
        List<String> capturedTables = tables.getAllValues();
        assertEquals(3, capturedTables.stream().filter(
                HugeServerTables.TEMPORAL_HISTORY_TABLE::equals).count());
        assertEquals(1, capturedTables.stream().filter(
                HugeServerTables.TEMPORAL_CURRENT_TABLE::equals).count());
        assertEquals(1, capturedTables.stream().filter(
                HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE::equals).count());
        assertEquals(1, capturedTables.stream().filter(
                HugeServerTables.TEMPORAL_INDEX_TABLE::equals).count());
        for (int i = 0; i < capturedTables.size(); i++) {
            String table = capturedTables.get(i);
            if (HugeServerTables.TEMPORAL_CURRENT_TABLE.equals(table) ||
                HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE.equals(table) ||
                HugeServerTables.TEMPORAL_INDEX_TABLE.equals(table)) {
                assertEquals(revision, ByteBuffer.wrap(values.getAllValues().get(i)).getLong());
            }
        }
    }

    private static byte[] intervalKey(TemporalMutationBundle bundle) {
        return TemporalIntervalCodec.intervalKey(bundle);
    }

    private static ScanIterator emptyScan() {
        ScanIterator scan = mock(ScanIterator.class);
        when(scan.hasNext()).thenReturn(false);
        return scan;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
