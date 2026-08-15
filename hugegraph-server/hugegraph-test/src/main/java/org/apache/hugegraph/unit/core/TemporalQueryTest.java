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

import org.apache.hugegraph.temporal.TemporalGranularity;
import org.apache.hugegraph.temporal.TemporalInterval;
import org.apache.hugegraph.temporal.TemporalQuery;
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
}
