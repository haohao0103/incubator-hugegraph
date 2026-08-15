/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.TableMethod;
import org.apache.hugegraph.store.grpc.common.Tk;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.GetReq;
import org.apache.hugegraph.store.grpc.session.HgStoreSessionGrpc;
import org.apache.hugegraph.store.grpc.session.TableReq;
import org.apache.hugegraph.store.grpc.session.TemporalMutationReq;

import com.google.protobuf.ByteString;

import org.junit.Test;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Slice 1 primary evidence: temporal serialized arbitration lives in Store.
 *
 * The request path is gRPC -> PD-resolved partition leader -> Raft proposal ->
 * majority apply, bypassing GraphTransaction/core pre-validation entirely.
 * Two overlapping intervals submitted concurrently must yield exactly one
 * success and one TEMPORAL_CONFLICT; re-submitting the same mutation_id must be
 * a no-op. This class is a deterministic fault-injection entry point, not a
 * supported client API.
 */
public class TemporalStoreDirectConcurrencyTest {

    private static final String PD_ADDRESS = System.getProperty("temporal.test.pd", "127.0.0.1:8686");
    private static final PDClient PD_CLIENT = PDClient.create(PDConfig.of(PD_ADDRESS));
    private static final String GRAPH = System.getProperty("temporal.test.graph", "DEFAULT/hugegraph/g");
    private static final String LABEL = "driver_order_rel";
    private static final String ENTITY = "driver_1001";
    private static final byte[] FACT_KEY =
            ("fact-" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {
        List<String> results = run();
        results.forEach(System.out::println);
    }

    @Test
    public void shouldArbitrateConflictInStore() throws Exception {
        for (int round = 1; round <= 20; round++) {
            System.out.println("=== PRIMARY_TOCTOU_ROUND " + round + "/20 ===");
            List<String> results = run();
            results.forEach(System.out::println);
        }
    }

    public static List<String> run() throws Exception {
        List<String> log = new ArrayList<>();
        PDClient pd = PD_CLIENT;

        // 1. Resolve the partition that owns this fact key and its leader
        //    store through PD (real routing path, not fabricated). The write
        //    and the read both key off the fact-key hash, so they must agree.
        int keyCode = PartitionUtils.calcHashcode(FACT_KEY);
        var partShard = pd.getPartitionByCode(GRAPH, keyCode);
        int partId = partShard.getKey().getId();
        long leaderStoreId = partShard.getValue().getStoreId();
        Metapb.Store leaderStore = pd.getStore(leaderStoreId);
        String containerAddr = leaderStore.getAddress();
        String hostAddr = mapToHost(containerAddr);
        log.add("factKeyHash=" + keyCode + " PD partition=" + partId +
                " leaderStoreId=" + leaderStoreId +
                " containerAddr=" + containerAddr + " hostAddr=" + hostAddr);

        // 2. Connect to the leader store gRPC endpoint.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostAddr)
                                                      .usePlaintext()
                                                      .build();
        HgStoreSessionGrpc.HgStoreSessionBlockingStub stub =
                HgStoreSessionGrpc.newBlockingStub(channel);
        Header header = Header.newBuilder().setGraph(GRAPH).build();

        // 3. Reset temporal tables (drop leftovers from prior runs) and
        //    re-create them through the real Table RPC.
        List<String> tables = Arrays.asList(
                HugeServerTables.TEMPORAL_HISTORY_TABLE,
                HugeServerTables.TEMPORAL_CURRENT_TABLE,
                HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE,
                HugeServerTables.TEMPORAL_INDEX_TABLE);
        for (String table : tables) {
            try {
                stub.table(TableReq.newBuilder()
                                   .setHeader(header)
                                   .setMethod(TableMethod.TABLE_METHOD_DROP)
                                   .setTableName(table)
                                   .build());
            } catch (Exception ignored) {
                // table may not exist yet
            }
        }
        for (String table : tables) {
            FeedbackRes res = stub.table(TableReq.newBuilder()
                                                 .setHeader(header)
                                                 .setMethod(TableMethod.TABLE_METHOD_CREATE)
                                                 .setTableName(table)
                                                 .build());
            log.add("createTable " + table + " status=" +
                    res.getStatus().getCode() + " " + res.getStatus().getMsg());
        }

        // 4. Build bundles: A [100,200), B [150,250) overlap, A replay same id.
        TemporalMutationBundle a = bundle("mutation-a", 100L, 200L);
        TemporalMutationBundle b = bundle("mutation-b", 150L, 250L);

        // 5. Concurrently submit A and B to the same leader partition. The
        //    Store serializes them: whichever applies first succeeds, the other
        //    must be rejected with TEMPORAL_CONFLICT.
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<FeedbackRes> fA = pool.submit(() -> {
            start.await();
            return submit(stub, header, keyCode, a);
        });
        Future<FeedbackRes> fB = pool.submit(() -> {
            start.await();
            return submit(stub, header, keyCode, b);
        });
        start.countDown();
        FeedbackRes resA = fA.get(30, TimeUnit.SECONDS);
        FeedbackRes resB = fB.get(30, TimeUnit.SECONDS);
        log.add("A[100,200) status=" + resA.getStatus().getCode() + " " +
                resA.getStatus().getMsg());
        log.add("B[150,250) status=" + resB.getStatus().getCode() + " " +
                resB.getStatus().getMsg());
        pool.shutdownNow();

        boolean aOk = resA.getStatus().getCode() == ResCode.RES_CODE_OK;
        boolean bOk = resB.getStatus().getCode() == ResCode.RES_CODE_OK;
        boolean aConflict = resA.getStatus().getMsg().contains("TEMPORAL_CONFLICT");
        boolean bConflict = resB.getStatus().getMsg().contains("TEMPORAL_CONFLICT");
        boolean oneSuccess = aOk ^ bOk;
        boolean oneConflict = aConflict ^ bConflict;
        log.add("ASSERT oneSuccess=" + oneSuccess + " oneConflict=" + oneConflict);

        // 6. The winner is whichever applied. Replaying it with the same
        //    mutation_id must be a no-op (OK), and its interval must be
        //    present in the history table.
        TemporalMutationBundle winner = aOk ? a : b;
        FeedbackRes replay = submit(stub, header, keyCode, winner);
        boolean replayNoop = replay.getStatus().getCode() == ResCode.RES_CODE_OK;
        log.add("winner-replay mutationId=" + winner.mutationId() +
                " status=" + replay.getStatus().getCode() + " " +
                replay.getStatus().getMsg());

        // 7. Read back the winner's interval marker to confirm it was applied.
        // The Get code must be the fact-key hash so BusinessHandler resolves
        // the same partition and reproduces the same key suffix as the write.
        int readCode = PartitionUtils.calcHashcode(FACT_KEY);
        byte[] intervalKey = intervalKey(winner);
        FeedbackRes get = stub.get2(GetReq.newBuilder()
                                          .setHeader(header)
                                          .setTk(Tk.newBuilder()
                                                   .setTable(HugeServerTables
                                                             .TEMPORAL_HISTORY_TABLE)
                                                   .setKey(ByteString.copyFrom(intervalKey))
                                                   .setCode(readCode)
                                                   .build())
                                          .build());
        boolean hasInterval = get.getStatus().getCode() == ResCode.RES_CODE_OK &&
                              get.getValueResponse().getValue().size() > 0;
        log.add("history interval marker present=" + hasInterval +
                " getStatus=" + get.getStatus().getCode());

        // 8. Final assertions.
        log.add("ASSERT oneSuccess=" + oneSuccess +
                " oneConflict=" + oneConflict +
                " replayNoop=" + replayNoop +
                " historyIntervalPresent=" + hasInterval);
        if (!oneSuccess || !oneConflict || !replayNoop || !hasInterval) {
            log.add("RESULT=FAIL");
            throw new AssertionError(String.join("\n", log));
        }
        log.add("RESULT=PASS");
        channel.shutdownNow();
        return log;
    }

    private static FeedbackRes submit(HgStoreSessionGrpc.HgStoreSessionBlockingStub stub,
                                      Header header, int code,
                                      TemporalMutationBundle bundle) throws Exception {
        byte[] encoded = TemporalMutationBundleCodec.encode(bundle);
        return stub.temporalMutation(TemporalMutationReq.newBuilder()
                                                        .setHeader(header)
                                                        .setCode(code)
                                                        .setBundle(ByteString.copyFrom(encoded))
                                                        .build());
    }

    private static TemporalMutationBundle bundle(String id, long from, long to) {
        List<TemporalMutationBundle.ViewMutation> views = Arrays.asList(
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_HISTORY_TABLE, bytes("h"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_CURRENT_TABLE, bytes("c"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_OPEN_INDEX_TABLE, bytes("o"), bytes("v")),
                new TemporalMutationBundle.ViewMutation(
                        HugeServerTables.TEMPORAL_INDEX_TABLE, bytes("i"), bytes("v")));
        return new TemporalMutationBundle(GRAPH, LABEL, ENTITY, FACT_KEY, id,
                                          TemporalWireProtocol.SCHEMA_VERSION,
                                          from, to, false, bytes("payload"), views);
    }

    private static byte[] intervalKey(TemporalMutationBundle bundle) {
        return TemporalIntervalCodec.intervalKey(bundle);
    }

    private static String mapToHost(String containerAddr) {
        // Local cluster: addresses may already be 127.0.0.1:<port> (manual
        // single-node) or container hostnames storeN:8500 (docker). Remap only
        // the latter.
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
