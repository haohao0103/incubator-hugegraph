/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.unit.store;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the HBase RowIterator resource cleanup behavior,
 * including the finalize() safety net for ResultScanner leaks.
 *
 * RowIterator is a protected static class inside HbaseSessions,
 * tested via reflection to validate close() and finalize() methods.
 */
@SuppressWarnings("removal")
public class RowIteratorCleanupTest extends BaseUnitTest {

    @Test
    public void testClosePropagatesToResultScanner() throws Exception {
        AtomicBoolean scannerClosed = new AtomicBoolean(false);
        ResultScanner mockScanner = createMockScanner(scannerClosed);

        Object rowIterator = createRowIteratorWithScanner(mockScanner);
        Method closeMethod = rowIterator.getClass().getMethod("close");

        closeMethod.invoke(rowIterator);

        Assert.assertTrue(
                "RowIterator.close() must call ResultScanner.close()",
                scannerClosed.get());
    }

    @Test
    public void testFinalizeClosesResultScanner() throws Exception {
        AtomicBoolean scannerClosed = new AtomicBoolean(false);
        ResultScanner mockScanner = createMockScanner(scannerClosed);

        Object rowIterator = createRowIteratorWithScanner(mockScanner);

        // Invoke finalize directly for reliable testing
        Method finalizeMethod = rowIterator.getClass()
                .getDeclaredMethod("finalize");
        finalizeMethod.setAccessible(true);
        finalizeMethod.invoke(rowIterator);

        Assert.assertTrue(
                "RowIterator.finalize() must close the ResultScanner " +
                "as a GC safety net",
                scannerClosed.get());
    }

    @Test
    public void testExhaustionAutoClosesScanner() throws Exception {
        AtomicBoolean scannerClosed = new AtomicBoolean(false);

        // Create a mock scanner that returns one result, then empty
        Result mockResult = Mockito.mock(Result.class);
        Iterator<Result> iter = Arrays.asList(mockResult).iterator();

        ResultScanner mockScanner = Mockito.mock(ResultScanner.class);
        Mockito.when(mockScanner.iterator()).thenReturn(iter);
        Mockito.when(mockScanner.next()).thenReturn(mockResult);
        Mockito.doAnswer(invocation -> {
            scannerClosed.set(true);
            return null;
        }).when(mockScanner).close();

        Object rowIterator = createRowIteratorWithScanner(mockScanner);

        Method hasNextMethod = rowIterator.getClass().getMethod("hasNext");
        Method nextMethod = rowIterator.getClass().getMethod("next");

        // Consume the one result
        Assert.assertTrue((Boolean) hasNextMethod.invoke(rowIterator));
        nextMethod.invoke(rowIterator);

        // Now iteration is exhausted → must auto-close
        Assert.assertFalse((Boolean) hasNextMethod.invoke(rowIterator));
        Assert.assertTrue(
                "RowIterator must auto-close ResultScanner on exhaustion",
                scannerClosed.get());
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        AtomicBoolean scannerClosed = new AtomicBoolean(false);
        ResultScanner mockScanner = createMockScanner(scannerClosed);

        Object rowIterator = createRowIteratorWithScanner(mockScanner);
        Method closeMethod = rowIterator.getClass().getMethod("close");

        // Call close twice; second call must not throw
        closeMethod.invoke(rowIterator);
        closeMethod.invoke(rowIterator);
        // No exception = pass
    }

    @Test
    public void testCloseWithNullScannerSafe() throws Exception {
        // Test the get-by-id constructor path (no ResultScanner)
        Result mockResult = Mockito.mock(Result.class);
        Mockito.when(mockResult.isEmpty()).thenReturn(false);

        Object rowIterator = createRowIteratorWithResults(mockResult);
        Method closeMethod = rowIterator.getClass().getMethod("close");

        // Must not throw even though resultScanner is null
        closeMethod.invoke(rowIterator);
    }

    // ---- Helpers ----

    private static ResultScanner createMockScanner(AtomicBoolean closed) {
        ResultScanner scanner = Mockito.mock(ResultScanner.class);
        Mockito.doAnswer(invocation -> {
            closed.set(true);
            return null;
        }).when(scanner).close();
        return scanner;
    }

    private static Object createRowIteratorWithScanner(ResultScanner scanner)
            throws Exception {
        String className = "org.apache.hugegraph.backend.store.hbase." +
                          "HbaseSessions$RowIterator";
        Class<?> clazz = Class.forName(className);
        Constructor<?> ctor = clazz.getDeclaredConstructor(ResultScanner.class);
        ctor.setAccessible(true);
        return ctor.newInstance(scanner);
    }

    private static Object createRowIteratorWithResults(Result... results)
            throws Exception {
        String className = "org.apache.hugegraph.backend.store.hbase." +
                          "HbaseSessions$RowIterator";
        Class<?> clazz = Class.forName(className);
        Constructor<?> ctor = clazz.getDeclaredConstructor(Result[].class);
        ctor.setAccessible(true);
        return ctor.newInstance((Object) results);
    }
}
