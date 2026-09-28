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
package org.apache.hugegraph.unit.core;

import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.temporal.TemporalGranularity;
import org.apache.hugegraph.temporal.TemporalInterval;
import org.apache.hugegraph.temporal.TemporalQuery;
import org.apache.hugegraph.temporal.store.TemporalIntervalResult;
import org.apache.hugegraph.temporal.store.TemporalQueryPage;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class TemporalQueryTest extends BaseUnitTest {

    @Test
    public void testAsOfBetweenAndOverlapBoundaries() {
        TemporalInterval interval = new TemporalInterval(
                "person-1", "employment", "company", 100L, 200L,
                TemporalGranularity.SECOND);

        Assert.assertTrue(TemporalQuery.asOf(100L).matches(interval));
        Assert.assertTrue(TemporalQuery.asOf(199L).matches(interval));
        Assert.assertFalse(TemporalQuery.asOf(200L).matches(interval));
        Assert.assertTrue(TemporalQuery.between(50L, 101L).matches(interval));
        Assert.assertFalse(TemporalQuery.between(200L, 300L).matches(interval));
        Assert.assertTrue(TemporalQuery.overlap(199L, 300L).matches(interval));
        Assert.assertFalse(TemporalQuery.overlap(200L, 300L).matches(interval));
    }

    @Test
    public void testRangeQueriesRejectEmptyAndReverseRanges() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                TemporalQuery.between(100L, 100L));
        Assert.assertThrows(IllegalArgumentException.class, () ->
                TemporalQuery.overlap(200L, 100L));
    }

    @Test
    public void testWithPageCarriesLimitAndCursor() {
        // Defaults keep the pre-Phase-4 behavior: no cap, empty cursor.
        TemporalQuery base = TemporalQuery.between(100L, 200L);
        Assert.assertEquals(0L, base.limit());
        Assert.assertEquals(0, base.pageToken().length);
        Assert.assertFalse(base.hasPageToken());

        byte[] cursor = new byte[]{0, 0, 0, 0, 0, 0, 0, 100};
        TemporalQuery paged = base.withPage(10L, cursor);
        // The type and window are preserved; only the page fields are added.
        Assert.assertEquals(TemporalQuery.Type.BETWEEN, paged.type());
        Assert.assertEquals(100L, paged.from());
        Assert.assertEquals(200L, paged.to().longValue());
        Assert.assertEquals(10L, paged.limit());
        Assert.assertTrue(paged.hasPageToken());
        Assert.assertArrayEquals(cursor, paged.pageToken());

        // withPage returns a copy; the source query stays immutable.
        Assert.assertEquals(0L, base.limit());
        Assert.assertFalse(base.hasPageToken());
    }

    @Test
    public void testWithPageRejectsNegativeLimit() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                TemporalQuery.between(100L, 200L).withPage(-1L, new byte[0]));
    }

    @Test
    public void testQueryPageExposesIntervalsCursorAndScannedRows() {
        List<TemporalIntervalResult> intervals = Collections.singletonList(
                new TemporalIntervalResult(new byte[0], 100L, 200L, 7L));
        TemporalQueryPage page = new TemporalQueryPage(intervals,
                                                       new byte[]{1, 2, 3}, 42L);
        Assert.assertEquals(1, page.intervals().size());
        Assert.assertEquals(42L, page.scannedRows());
        Assert.assertTrue(page.hasMore());
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, page.nextPageToken());

        // An empty cursor means the result set is complete.
        TemporalQueryPage last = TemporalQueryPage.of(intervals);
        Assert.assertFalse(last.hasMore());
        Assert.assertEquals(0, last.nextPageToken().length);
        Assert.assertEquals(0L, last.scannedRows());
    }
}
