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

import java.util.List;

import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.hbase.HbaseSessions;
import org.apache.hugegraph.backend.store.hbase.HbaseStore;
import org.apache.hugegraph.backend.store.hbase.HbaseTables;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;

/**
 * OBKV backend using HugeGraph's existing HBase-compatible table layout.
 * HBase backend behavior remains unchanged; this type only swaps the
 * connection implementation and configuration source.
 */
public abstract class ObkvStore extends HbaseStore {

    protected ObkvStore(BackendStoreProvider provider, String namespace,
                        String store, boolean enablePartition) {
        super(provider, namespace, store, enablePartition);
    }

    @Override
    protected ObkvSessions createSessions(HugeConfig config) {
        ObkvSessions.validate(config);
        return new ObkvSessions(config, this.namespace(), this.store());
    }

    @Override
    public void init() {
        // OBKV tables are expected to be created by the deployment DDL job.
        // The current 2.5.0 client does not expose a stable HBase 2.x Admin
        // contract that is safe to use without a live cluster verification.
        super.init();
    }

    public static class ObkvSchemaStore extends ObkvStore {

        private final HbaseTables.Counters counters;

        public ObkvSchemaStore(HugeConfig config, BackendStoreProvider provider,
                               String namespace, String store) {
            super(provider, namespace, store,
                  config.get(ObkvOptions.ENABLE_PARTITION));
            this.counters = new HbaseTables.Counters();
            registerTableManager(HugeType.VERTEX_LABEL, new HbaseTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL, new HbaseTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY, new HbaseTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL, new HbaseTables.IndexLabel());
            registerTableManager(HugeType.SECONDARY_INDEX, new HbaseTables.SecondaryIndex(store));
        }

        @Override
        protected List<String> tableNames() {
            List<String> names = super.tableNames();
            names.add(this.counters.table());
            return names;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            super.checkOpened();
            this.counters.increaseCounter(super.session(null), type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            super.checkOpened();
            return this.counters.getCounter(super.session(null), type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class ObkvGraphStore extends ObkvStore {

        public ObkvGraphStore(HugeConfig config, BackendStoreProvider provider,
                              String namespace, String store) {
            super(provider, namespace, store,
                  config.get(ObkvOptions.ENABLE_PARTITION));
            boolean partition = config.get(ObkvOptions.ENABLE_PARTITION);
            registerTableManager(HugeType.VERTEX, new HbaseTables.Vertex(store, partition));
            registerTableManager(HugeType.EDGE_OUT, HbaseTables.Edge.out(store, partition));
            registerTableManager(HugeType.EDGE_IN, HbaseTables.Edge.in(store, partition));
            registerTableManager(HugeType.SECONDARY_INDEX, new HbaseTables.SecondaryIndex(store));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX, new HbaseTables.VertexLabelIndex(store));
            registerTableManager(HugeType.EDGE_LABEL_INDEX, new HbaseTables.EdgeLabelIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX, HbaseTables.RangeIndex.rangeInt(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX, HbaseTables.RangeIndex.rangeFloat(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX, HbaseTables.RangeIndex.rangeLong(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX, HbaseTables.RangeIndex.rangeDouble(store));
            registerTableManager(HugeType.SEARCH_INDEX, new HbaseTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX, new HbaseTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX, new HbaseTables.UniqueIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            throw new UnsupportedOperationException("ObkvGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException("ObkvGraphStore.getCounter()");
        }
    }

    public static class ObkvSystemStore extends ObkvGraphStore {

        private final HbaseTables.Meta meta;

        public ObkvSystemStore(HugeConfig config, BackendStoreProvider provider,
                               String namespace, String store) {
            super(config, provider, namespace, store);
            this.meta = new HbaseTables.Meta();
        }

        @Override
        protected List<String> tableNames() {
            List<String> names = super.tableNames();
            names.add(this.meta.table());
            return names;
        }

        @Override
        public void init() {
            super.init();
            HbaseSessions.Session session = super.session(null);
            this.meta.writeVersion(session, this.provider().driverVersion());
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }
    }
}
