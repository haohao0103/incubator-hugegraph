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

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class ObkvSerializer extends BinarySerializer {

    private static final Logger LOG = Log.logger(ObkvSerializer.class);
    private final short vertexPartitions;
    private final short edgePartitions;

    public ObkvSerializer(HugeConfig config) {
        super(false, true, config.get(ObkvOptions.ENABLE_PARTITION));
        this.vertexPartitions = config.get(ObkvOptions.VERTEX_PARTITIONS).shortValue();
        this.edgePartitions = config.get(ObkvOptions.EDGE_PARTITIONS).shortValue();
        LOG.debug("OBKV vertex partitions: {}, edge partitions: {}",
                  this.vertexPartitions, this.edgePartitions);
    }

    @Override
    protected short getPartition(HugeType type, Id id) {
        int hashcode = Arrays.hashCode(id.asBytes());
        short partitions = type.isEdge() ? this.edgePartitions : this.vertexPartitions;
        short partition = (short) (hashcode % partitions);
        return partition > 0 ? partition : (short) -partition;
    }
}
