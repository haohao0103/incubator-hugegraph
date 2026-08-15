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

import java.util.Arrays;
import java.util.LinkedHashSet;

import org.apache.hugegraph.temporal.TemporalGranularity;
import org.apache.hugegraph.temporal.TemporalInterval;
import org.apache.hugegraph.temporal.TemporalMutation;
import org.apache.hugegraph.temporal.TemporalQuery;
import org.apache.hugegraph.temporal.TemporalSchema;
import org.apache.hugegraph.temporal.TemporalTime;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class TemporalModelTest extends BaseUnitTest {

    @Test
    public void testParseAndFormatUtcAndEpochMillis() {
        long epoch = TemporalTime.parse("2024-01-01T00:00:00.123Z");
        Assert.assertEquals(1704067200123L, epoch);
        Assert.assertEquals("2024-01-01T00:00:00.123Z", TemporalTime.format(epoch));
        Assert.assertEquals(epoch, TemporalTime.parse(epoch));
    }

    @Test
    public void testIntervalHalfOpenBoundaryAndOverlap() {
        TemporalInterval first = interval(100L, 200L);
        TemporalInterval adjacent = interval(200L, 300L);
        TemporalInterval overlap = interval(199L, 300L);

        Assert.assertTrue(first.contains(100L));
        Assert.assertTrue(first.contains(199L));
        Assert.assertFalse(first.contains(200L));
        Assert.assertFalse(first.overlaps(adjacent));
        Assert.assertTrue(first.overlaps(overlap));
    }

    @Test
    public void testOpenIntervalAndInvalidBounds() {
        TemporalInterval open = interval(100L, null);
        Assert.assertTrue(open.contains(Long.MAX_VALUE));
        Assert.assertThrows(IllegalArgumentException.class, () -> interval(100L, 100L));
        Assert.assertThrows(IllegalArgumentException.class, () -> interval(200L, 100L));
    }

    @Test
    public void testQuerySemantics() {
        TemporalInterval interval = interval(100L, 200L);
        Assert.assertTrue(TemporalQuery.asOf(100L).matches(interval));
        Assert.assertTrue(TemporalQuery.between(50L, 101L).matches(interval));
        Assert.assertTrue(TemporalQuery.overlap(199L, 300L).matches(interval));
        Assert.assertFalse(TemporalQuery.between(200L, 300L).matches(interval));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> TemporalQuery.between(100L, 100L));
    }

    @Test
    public void testSchemaAndMutationContracts() {
        TemporalSchema schema = new TemporalSchema(
                "employment", "person", "employment",
                new LinkedHashSet<>(Arrays.asList("company")));
        TemporalInterval interval = interval(100L, null);
        schema.check(interval);
        TemporalMutation mutation = new TemporalMutation(
                "m-1", TemporalMutation.Operation.APPEND, interval, "idem-1");
        Assert.assertEquals("m-1", mutation.mutationId());
        Assert.assertEquals(TemporalMutation.Operation.APPEND, mutation.operation());
        Assert.assertEquals("idem-1", mutation.idempotencyKey());

        Assert.assertThrows(IllegalArgumentException.class, () ->
                new TemporalMutation("m-2", TemporalMutation.Operation.APPEND,
                                     interval, ""));
    }

    @Test
    public void testSchemaRejectsUnknownTemporalKey() {
        TemporalSchema schema = new TemporalSchema(
                "employment", "person", "employment",
                new LinkedHashSet<>(Arrays.asList("company")));
        TemporalInterval interval = new TemporalInterval(
                "person-1", "employment", "title", 100L, null,
                TemporalGranularity.SECOND);
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> schema.check(interval));
    }

    private static TemporalInterval interval(long from, Long to) {
        return new TemporalInterval("person-1", "employment", "company",
                                    from, to, TemporalGranularity.SECOND);
    }
}
