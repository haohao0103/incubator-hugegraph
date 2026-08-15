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
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendIterator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for BinaryEntryIterator resource cleanup behavior,
 * verifying the close chain and the eager close on exhaustion
 * (the Cleaner is a last-resort safety net for abandoned iterators).
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
    public void testExhaustionClosesUnderlyingIterator() throws Exception {
        TrackingIterator tracker = new TrackingIterator(
                new byte[]{1}, new byte[]{2}, new byte[]{3});
        Query query = new Query(HugeType.VERTEX);
        BinaryEntryIterator<byte[]> it = new BinaryEntryIterator<>(
                tracker, query, (entry, elem) -> mockEntry());

        // Fully consume the iterator; exhausting the backend must close the
        // underlying iterator eagerly instead of waiting for GC + Cleaner.
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }

        Assert.assertEquals(3, count);
        Assert.assertEquals(3, tracker.getNextCount());
        Assert.assertTrue(
                "Exhausted BinaryEntryIterator must close the wrapped iterator",
                tracker.isClosed());
    }

    private static BackendEntry mockEntry() {
        BackendEntry entry = mock(BackendEntry.class);
        when(entry.type()).thenReturn(HugeType.VERTEX);
        return entry;
    }
}
