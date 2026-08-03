/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hugegraph.backend.store.obkv;

import java.util.Arrays;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ObkvSerializerTest {

    @Test
    public void testPartitionIsStableAndWithinConfiguredRange() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.vertex_partitions", "10");
        properties.setProperty("obkv.edge_partitions", "30");
        ObkvSerializer serializer = new ObkvSerializer(new HugeConfig(properties));
        TestSerializer exposed = new TestSerializer(new HugeConfig(properties));

        short vertex = exposed.partition(HugeType.VERTEX,
                                         IdGenerator.of("vertex-1"));
        short edge = exposed.partition(HugeType.EDGE_OUT,
                                       IdGenerator.of("edge-1"));
        assertEquals(vertex, exposed.partition(HugeType.VERTEX,
                                               IdGenerator.of("vertex-1")));
        org.junit.Assert.assertTrue(vertex >= 0 && vertex < 10);
        org.junit.Assert.assertTrue(edge >= 0 && edge < 30);
        assertEquals(serializer.getClass(), ObkvSerializer.class);
    }

    private static class TestSerializer extends ObkvSerializer {

        TestSerializer(HugeConfig config) {
            super(config);
        }

        short partition(HugeType type, org.apache.hugegraph.backend.id.Id id) {
            return getPartition(type, id);
        }
    }
}
