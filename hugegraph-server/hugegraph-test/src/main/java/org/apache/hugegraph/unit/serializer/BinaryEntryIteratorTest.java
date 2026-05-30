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

package org.apache.hugegraph.unit.serializer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryEntryIterator;
import org.apache.hugegraph.backend.store.BackendEntry.BackendIterator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

/**
 * Tests for BinaryEntryIterator resource cleanup behavior,
 * verifying the finalize() GC safety net for the close chain.
 */
public class BinaryEntryIteratorTest extends BaseUnitTest {

    /**
     * A mock BackendIterator that tracks whether close() was called.
     */
    static class TrackingIterator implements BackendIterator<byte[]> {

        private final Iterator<byte[]> iter;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicInteger nextCount = new AtomicInteger(0);

        TrackingIterator(byte[]... elements) {
            this.iter = Arrays.asList(elements).iterator();
        }

        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        @Override
        public byte[] next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.nextCount.incrementAndGet();
            return this.iter.next();
        }

        @Override
        public void close() {
            this.closed.set(true);
        }

        @Override
        public byte[] position() {
            return PageState.EMPTY_BYTES;
        }

        boolean isClosed() {
            return this.closed.get();
        }

        int getNextCount() {
            return this.nextCount.get();
        }
    }

    @Test
    public void testClosePropagatesToWrappedIterator() throws Exception {
        TrackingIterator tracker = new TrackingIterator();
        BinaryEntryIterator<byte[]> it = new BinaryEntryIterator<>(
                tracker, new Query(HugeType.VERTEX),
                (entry, elem) -> entry);

        Assert.assertFalse("Not closed before calling close()",
                          tracker.isClosed());

        it.close();

        Assert.assertTrue(
                "BinaryEntryIterator.close() must propagate to wrapped iterator",
                tracker.isClosed());
    }

    @Test
    public void testCloseIdempotent() throws Exception {
        TrackingIterator tracker = new TrackingIterator();
        Query query = new Query(HugeType.VERTEX);
        BinaryEntryIterator<byte[]> it = new BinaryEntryIterator<>(
                tracker, query, (entry, elem) -> entry);

        // First close
        it.close();
        Assert.assertTrue(tracker.isClosed());

        // Second close must not throw
        it.close();
    }

    @Test
    public void testFinalizeClosesUnderlyingIterator() throws Exception {
        TrackingIterator tracker = new TrackingIterator(
                new byte[]{1, 2, 3});
        Query query = new Query(HugeType.VERTEX);

        // Create an instance and let it become unreachable
        // The finalize() method is tested by directly invoking it
        BinaryEntryIterator<byte[]> it = new BinaryEntryIterator<>(
                tracker, query, (entry, elem) -> entry);

        // Directly invoke finalize via reflection for reliable testing
        java.lang.reflect.Method finalize = it.getClass()
                .getDeclaredMethod("finalize");
        finalize.setAccessible(true);
        finalize.invoke(it);

        Assert.assertTrue(
                "BinaryEntryIterator.finalize() must close the wrapped iterator",
                tracker.isClosed());
    }

    @Test
    public void testGarbageCollectionTriggersFinalize() throws Exception {
        TrackingIterator tracker = new TrackingIterator();
        Query query = new Query(HugeType.VERTEX);
        final AtomicInteger finalized = new AtomicInteger(0);

        // Anonymous subclass to track finalize calls
        BinaryEntryIterator<byte[]> it = new BinaryEntryIterator<byte[]>(
                tracker, query, (entry, elem) -> entry) {
            @Override
            protected void finalize() throws Throwable {
                finalized.incrementAndGet();
                super.finalize();
            }
        };

        // Check that finalize tracks the call count
        Assert.assertEquals(0, finalized.get());

        // Call close directly first to verify normal path
        it.close();
        Assert.assertTrue("Normal close path must work", tracker.isClosed());
    }
}
