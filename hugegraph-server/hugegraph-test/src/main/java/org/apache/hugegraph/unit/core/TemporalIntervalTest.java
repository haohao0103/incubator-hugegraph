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
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class TemporalIntervalTest extends BaseUnitTest {

    @Test
    public void testHalfOpenMillisecondsAndOpenEnd() {
        TemporalInterval interval = new TemporalInterval(
                "person-1", "employment", "company", 1000L, 2000L,
                TemporalGranularity.MILLIS);

        Assert.assertTrue(interval.contains(1000L));
        Assert.assertTrue(interval.contains(1999L));
        Assert.assertFalse(interval.contains(2000L));
        Assert.assertFalse(interval.overlaps(new TemporalInterval(
                "person-1", "employment", "company", 2000L, 3000L,
                TemporalGranularity.MILLIS)));
        Assert.assertTrue(interval.overlaps(new TemporalInterval(
                "person-1", "employment", "company", 1999L, 3000L,
                TemporalGranularity.MILLIS)));

        TemporalInterval open = new TemporalInterval(
                "person-1", "employment", "company", 2000L, null,
                TemporalGranularity.MILLIS);
        Assert.assertTrue(open.contains(Long.MAX_VALUE));
    }

    @Test
    public void testInvalidBoundsAndRequiredIdentity() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                new TemporalInterval("", "employment", "company", 1L, 2L,
                                     TemporalGranularity.SECOND));
        Assert.assertThrows(IllegalArgumentException.class, () ->
                new TemporalInterval("person-1", "employment", "company", 2L, 2L,
                                     TemporalGranularity.SECOND));
        Assert.assertThrows(IllegalArgumentException.class, () ->
                new TemporalInterval("person-1", "employment", "company", 3L, 2L,
                                     TemporalGranularity.SECOND));
    }

    @Test
    public void testEntityLabelIsNotDuplicatedInIntervalModel() {
        TemporalInterval interval = new TemporalInterval(
                "person-1", "employment", "company", 1L, 2L,
                TemporalGranularity.SECOND);
        Assert.assertEquals("person-1", interval.entityId());
        Assert.assertEquals("employment", interval.temporalLabel());
    }
}
