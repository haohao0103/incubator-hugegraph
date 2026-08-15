/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.hugegraph.store.temporal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.TableMethod;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.HgStoreSessionGrpc;
import org.apache.hugegraph.store.grpc.session.TableReq;
import org.apache.hugegraph.store.grpc.session.TemporalInterval;
import org.apache.hugegraph.store.grpc.session.TemporalMutationReq;
import org.apache.hugegraph.store.grpc.session.TemporalQueryReq;
import org.apache.hugegraph.store.grpc.session.TemporalQueryRes;
import org.apache.hugegraph.store.grpc.session.TemporalQueryType;

import com.google.protobuf.ByteString;

import org.junit.Assert;
import org.junit.Test;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Store-level append -&gt; as_of round-trip over the temporalMutation /
 * temporalQuery RPCs. It proves the query path reads back what the write path
 * applied; it is an integration test and requires a running HStore cluster.
 */
public class TemporalStoreDirectQueryTest {

    private static final String PD_ADDRESS = System.getProperty("temporal.test.pd", "127.0.0.1:8686");
    private static final PDClient PD_CLIENT = PDClient.create(PDConfig.of(PD_ADDRESS));
    private static final String GRAPH = System.getProperty("temporal.test.graph", "DEFAULT/hugegraph/g");
    private static final String LABEL = "driver_order_rel";
    private static final String ENTITY = "driver_1001";
    private static final byte[] FACT_KEY =
            ("fact-" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);

    @Test
    public void shouldRoundTripAppendThenAsOf() throws Exception {
        int keyCode = PartitionUtils.calcHashcode(FACT_KEY);
        var partShard = PD_CLIENT.getPartitionByCode(GRAPH, keyCode);
        int partId = partShard.getKey().getId();
        long leaderStoreId = partShard.getValue().getStoreId();
        Metapb.Store leaderStore = PD_CLIENT.getStore(leaderStoreId);
        String hostAddr = mapToHost(leaderStore.getAddress());

        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostAddr).usePlaintext().build();
        HgStoreSessionGrpc.HgStoreSessionBlockingStub stub =
                HgStoreSessionGrpc.newBlockingStub(channel);
        Header header = Header.newBuilder().setGraph(GRAPH).build();

        // Reset + create the four temporal tables.
        List<String> tables = Arrays.asList(
                HugeServerTables.TEMPORAL_HISTORY_TABLE,
                HugeServerTables.TEMPORAL_CURRENT_TABLE,
                HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE,
                HugeServerTables.TEMPORAL_INDEX_TABLE);
        for (String table : tables) {
            try {
                stub.table(TableReq.newBuilder().setHeader(header)
                                   .setMethod(TableMethod.TABLE_METHOD_DROP)
                                   .setTableName(table).build());
            } catch (Exception ignored) {
                // table may not exist yet
            }
        }
        for (String table : tables) {
            stub.table(TableReq.newBuilder().setHeader(header)
                               .setMethod(TableMethod.TABLE_METHOD_CREATE)
                               .setTableName(table).build());
        }

        // 1. Append [100, 200).
        TemporalMutationBundle bundle = bundle("m1", 100L, 200L);
        FeedbackRes write = stub.temporalMutation(TemporalMutationReq.newBuilder()
                .setHeader(header).setCode(keyCode)
                .setBundle(ByteString.copyFrom(TemporalMutationBundleCodec.encode(bundle)))
                .build());
        Assert.assertEquals(ResCode.RES_CODE_OK, write.getStatus().getCode());

        // 2. as_of(150) must return the single interval [100, 200).
        TemporalQueryRes read = stub.temporalQuery(TemporalQueryReq.newBuilder()
                .setHeader(header)
                .setFactKey(ByteString.copyFrom(FACT_KEY))
                .setType(TemporalQueryType.TEMPORAL_QUERY_AS_OF)
                .setFrom(150L).build());
        List<TemporalInterval> rows = read.getIntervalList();
        Assert.assertEquals(1, rows.size());
        Assert.assertArrayEquals(FACT_KEY, rows.get(0).getFactKey().toByteArray());
        Assert.assertEquals(100L, rows.get(0).getValidFrom());
        Assert.assertEquals(200L, rows.get(0).getValidTo());
        Assert.assertFalse(rows.get(0).getOpen());

        // 3. as_of in a gap (250) must be empty.
        TemporalQueryRes gap = stub.temporalQuery(TemporalQueryReq.newBuilder()
                .setHeader(header)
                .setFactKey(ByteString.copyFrom(FACT_KEY))
                .setType(TemporalQueryType.TEMPORAL_QUERY_AS_OF)
                .setFrom(250L).build());
        Assert.assertEquals(0, gap.getIntervalList().size());

        channel.shutdownNow();
    }

    private static TemporalMutationBundle bundle(String id, long from, long to) {
        List<TemporalMutationBundle.ViewMutation> views = new ArrayList<>();
        views.add(new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_HISTORY_TABLE, bytes("h"), bytes("v")));
        views.add(new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_CURRENT_TABLE, bytes("c"), bytes("v")));
        views.add(new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE, bytes("o"), bytes("v")));
        views.add(new TemporalMutationBundle.ViewMutation(
                HugeServerTables.TEMPORAL_INDEX_TABLE, bytes("i"), bytes("v")));
        return new TemporalMutationBundle(GRAPH, LABEL, ENTITY, FACT_KEY, id,
                                          TemporalWireProtocol.SCHEMA_VERSION,
                                          from, to, false, bytes("payload"), views);
    }

    private static String mapToHost(String containerAddr) {
        String host = containerAddr.split(":")[0];
        String port = containerAddr.split(":")[1];
        if (host.startsWith("127.") || "localhost".equals(host)) {
            return containerAddr;
        }
        if ("8500".equals(port)) {
            int idx = host.charAt(host.length() - 1) - '0';
            return "127.0.0.1:850" + idx;
        }
        return containerAddr;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
