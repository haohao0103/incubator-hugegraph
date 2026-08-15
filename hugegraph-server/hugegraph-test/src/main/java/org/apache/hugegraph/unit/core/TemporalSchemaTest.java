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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.hugegraph.temporal.TemporalGranularity;
import org.apache.hugegraph.temporal.TemporalInterval;
import org.apache.hugegraph.temporal.TemporalSchema;
import org.apache.hugegraph.temporal.TemporalSchemaRegistry;
import org.apache.hugegraph.temporal.store.TemporalFactKey;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class TemporalSchemaTest extends BaseUnitTest {

    @Test
    public void testSchemaChecksTemporalLabelAndFactKeyOnly() {
        TemporalSchema schema = schema();
        TemporalInterval interval = new TemporalInterval(
                "person-1", "employment", "company", 100L, 200L,
                TemporalGranularity.SECOND);

        schema.check(interval);
        Assert.assertEquals("person", schema.entityLabel());
        Assert.assertEquals("employment", schema.temporalLabel());
    }

    @Test
    public void testSchemaRejectsUnknownTemporalLabelOrFactKey() {
        TemporalSchema schema = schema();
        Assert.assertThrows(IllegalArgumentException.class, () -> schema.check(
                new TemporalInterval("person-1", "salary", "company", 1L, 2L,
                                     TemporalGranularity.SECOND)));
        Assert.assertThrows(IllegalArgumentException.class, () -> schema.check(
                new TemporalInterval("person-1", "employment", "title", 1L, 2L,
                                     TemporalGranularity.SECOND)));
    }

    @Test
    public void testSchemaRebuildsFactKeyFromDimensionOrder() {
        TemporalSchema schema = new TemporalSchema(
                "employment", "person", "employment",
                new LinkedHashSet<>(Arrays.asList("company")),
                Arrays.asList("subject_id", "object_id", "relation_type"));
        Assert.assertEquals(Arrays.asList("subject_id", "object_id", "relation_type"),
                            schema.dimensionOrder());

        Map<String, String> values = new LinkedHashMap<>();
        values.put("subject_id", "d1");
        values.put("object_id", "o1");
        values.put("relation_type", "drives");
        TemporalFactKey factKey = schema.factKey(values);
        Assert.assertEquals(Arrays.asList("subject_id", "object_id", "relation_type"),
                            factKey.dimensionOrder());
    }

    @Test
    public void testSchemaRejectsFactKeyWithoutDimensionOrder() {
        TemporalSchema schema = schema();
        Map<String, String> values = new LinkedHashMap<>();
        values.put("subject_id", "d1");
        Assert.assertThrows(IllegalStateException.class, () -> schema.factKey(values));
    }

    @Test
    public void testSchemaRegistryLookup() {
        TemporalSchemaRegistry registry = new TemporalSchemaRegistry();
        TemporalSchema schema = new TemporalSchema(
                "employment", "person", "employment",
                new LinkedHashSet<>(Arrays.asList("company")),
                Arrays.asList("subject_id", "object_id", "relation_type"));
        registry.register(schema);
        Assert.assertEquals(schema, registry.get("employment"));
        Assert.assertTrue(registry.contains("employment"));
        Assert.assertEquals(schema, registry.findByTemporalLabel("employment"));
        Assert.assertEquals(1, registry.all().size());
        Assert.assertNull(registry.get("missing"));
    }

    private static TemporalSchema schema() {
        return new TemporalSchema(
                "employment", "person", "employment",
                new LinkedHashSet<>(Arrays.asList("company")));
    }
}
