/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.hugegraph.store.pd;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.PDPulse;
import org.apache.hugegraph.pd.client.PDPulseImpl;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionType;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchGraphResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.meta.Graph;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.metric.HgMetricService;
import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultPdProvider implements PdProvider {
    private static final Logger LOG = Log.logger(DefaultPdProvider.class);
    private final PDClient pdClient;
    private final String pdServerAddress;
    private final PDPulse pulseClient;
    private Consumer<Throwable> hbOnError = null;
    private List<PartitionInstructionListener> partitionCommandListeners;
    private PDPulse.Notifier<PartitionHeartbeatRequest.Builder> pdPulse;
    private GraphManager graphManager = null;
    PDClient.PDEventListener listener = new PDClient.PDEventListener() {
        // 监听pd、store变更信息的listener
        @Override
        public void onStoreChanged(NodeEvent event) {
            // 如果事件类型是节点raft变更
            if (event.getEventType() == NodeEvent.EventType.NODE_RAFT_CHANGE) {
                // 打印日志，表示store raft group已变更
                log.info("store raft group changed!, {}", event);
                // 清除pd客户端的store缓存
                pdClient.invalidStoreCache(event.getNodeId());
                // 重建raft组
                HgStoreEngine.getInstance().rebuildRaftGroup(event.getNodeId());
            // 如果事件类型是pd leader变更
            } else if (event.getEventType() == NodeEvent.EventType.NODE_PD_LEADER_CHANGE) {
                // 打印日志，表示pd leader已变更，并重启心跳
                log.info("pd leader changed!, {}. restart heart beat", event);
                // 重置pulseClient的stub
                if (pulseClient.resetStub(event.getGraph(), pdPulse)) {
                    // 启动心跳流
                    startHeartbeatStream(hbOnError);
                }
            }
        }

        @Override
        public void onPartitionChanged(PartitionEvent event) {

        }

        @Override
        public void onGraphChanged(WatchResponse event) {
            WatchGraphResponse graphResponse = event.getGraphResponse();
            Metapb.Graph graph = graphResponse.getGraph();
            if (graphManager != null) {
                graphManager.updateGraph(new Graph(graph));
            }

        }
    };

    public DefaultPdProvider(String pdAddress) {
        this.pdClient = PDClient.create(PDConfig.of(pdAddress).setEnableCache(true));
        this.pdClient.addEventListener(listener);
        this.pdServerAddress = pdAddress;
        partitionCommandListeners = Collections.synchronizedList(new ArrayList());
        // pdClient.getLeaderIp() 就是与pd leader 通信的grpc 客户端
        log.info("pulse client connect to {}", pdClient.getLeaderIp());
        this.pulseClient = new PDPulseImpl(pdClient.getLeaderIp());
    }

    @Override
    public long registerStore(Store store) throws PDException {
        Asserts.isTrue(this.pdClient != null, "pd client is null");
        LOG.info("registerStore pd={} storeId={}, store={}", this.pdServerAddress, store.getId(),
                 store);

        long storeId = 0;
        Metapb.Store protoObj = store.getProtoObj();
        try {
            storeId = pdClient.registerStore(protoObj);
            store.setId(storeId);
            if (pdClient.getStore(storeId).getState() != Metapb.StoreState.Up) {
                LOG.warn("Store {} is not activated, state is {}", storeId,
                         pdClient.getStore(storeId).getState());
            }
        } catch (PDException e) {
            LOG.error(
                    "Exception in storage registration, StoreID= {} pd= {} exceptCode= {} except=" +
                    " {}.",
                    protoObj.getId(), this.pdServerAddress, e.getErrorCode(), e.getMessage());
            storeId = 0;
            throw e;
        } catch (Exception e) {
            LOG.error(
                    "Exception in storage registration, StoreID= {} pd= {} except= {}, Please " +
                    "check your network settings.",
                    protoObj.getId(), this.pdServerAddress, e.getMessage());
            handleCommonException(e);
            storeId = 0;
        }
        return storeId;
    }

    @Override
    public Partition getPartitionByID(String graph, int partId) {
        try {
            KVPair<Metapb.Partition, Metapb.Shard> pair = pdClient.getPartitionById(
                    graph, partId);
            if (null != pair) {
                return new Partition(pair.getKey());
            }
        } catch (PDException e) {
            log.error("Partition {}-{} getPartitionByID exception {}", graph, partId, e);
        }
        return null;
    }

    @Override
    public Metapb.Shard getPartitionLeader(String graph, int partId) {
        try {
            KVPair<Metapb.Partition, Metapb.Shard> pair = pdClient.getPartitionById(
                    graph, partId);
            if (null != pair) {
                return pair.getValue();
            }
        } catch (PDException e) {
            log.error("Partition {}-{} getPartitionByID exception {}", graph, partId, e);
        }
        return null;
    }

    @Override
    public Metapb.Partition getPartitionByCode(String graph, int code) {
        try {
            KVPair<Metapb.Partition, Metapb.Shard> pair = pdClient.getPartitionByCode(
                    graph, code);
            if (null != pair) {
                return pair.getKey();
            }
        } catch (PDException e) {
            log.error("Partition {} getPartitionByCode {} exception {}", graph, code, e);
        }
        return null;
    }

    @Override
    public Partition delPartition(String graph, int partId) {
        log.info("Partition {}-{} send delPartition to PD", graph, partId);
        try {
            Metapb.Partition partition = pdClient.delPartition(graph, partId);
            if (null != partition) {
                return new Partition(partition);
            }
        } catch (PDException e) {
            log.error("Partition {}-{} remove exception {}", graph, partId, e);
        }
        return null;
    }

    @Override
    public List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) throws
                                                                                     PDException {

        try {
            List<Metapb.Partition> results = pdClient.updatePartition(partitions);
            return results;
        } catch (PDException e) {
            throw e;
        }
    }

    /**
     * 根据给定的storeId获取对应的分区列表
     *
     * @param storeId 存储ID
     * @return 分区列表
     * @throws PDException 抛出PDException异常
     */
    @Override
    public List<Partition> getPartitionsByStore(long storeId) throws PDException {
        // 创建一个空的分区列表
        List<Partition> partitions = new ArrayList<>();
        // 通过pdClient获取指定storeId对应的分区列表
        List<Metapb.Partition> parts = pdClient.getPartitionsByStore(storeId);
        // 遍历每个分区
        parts.forEach(e -> {
            // 将每个分区转化为Partition对象，并添加到partitions列表中
            partitions.add(new Partition(e));
        });
        // 返回最终的分区列表
        return partitions;
    }

    @Override
    public void updatePartitionCache(Partition partition, Boolean changeLeader) {
        Metapb.Shard leader = null;

        var shardGroup = getShardGroup(partition.getId());
        if (shardGroup != null) {
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    leader = shard;
                }
            }
        }
        if (!changeLeader) {
            try {
                leader = pdClient.getPartitionById(partition.getGraphName(), partition.getId())
                                 .getValue();
            } catch (PDException e) {
                log.error("find leader error,leader changed to storeId:{}", leader.getStoreId());
            } catch (Exception e1) {
                log.error("exception ", e1);
            }
        }
        pdClient.updatePartitionCache(partition.getProtoObj(), leader);
    }

    @Override
    public void invalidPartitionCache(String graph, int partId) {
        pdClient.invalidPartitionCache(graph, partId);
    }

    /**
     * 启动partition心跳流式传输
     *
     * @return
     */
    @Override
    public boolean startHeartbeatStream(Consumer<Throwable> onError) {
        this.hbOnError = onError;
        pdPulse = pulseClient.connectPartition(new PDPulse.Listener<>() {

            @Override
            public void onNotice(PulseServerNotice<PulseResponse> response) {
                PulseResponse content = response.getContent();

                // 消息消费应答，能够正确消费消息，调用accept返回状态码，否则不要调用accept
                Consumer<Integer> consumer = integer -> {
                    LOG.debug("Partition heartbeat accept instruction: {}", content);
                    // http2 并发问题，需要加锁
                    // 同步代码块开始
                    synchronized (pdPulse) {
                        // LOG.info("accept notice id : {}, ts:{}", response.getNoticeId(), System
                        // .currentTimeMillis());
                        response.ack();
                    }
                    // 同步代码块结束
                };

                // 判断是否包含指令响应
                if (content.hasInstructionResponse()) {
                    var pdInstruction = content.getInstructionResponse();
                    consumer.accept(0);
                    // 判断指令类型是否为改变为follower
                    // 当前的链接变成了follower，重新链接
                    if (pdInstruction.getInstructionType() ==
                        PdInstructionType.CHANGE_TO_FOLLOWER) {
                        onCompleted();
                        log.info("got pulse instruction, change leader to {}",
                                 pdInstruction.getLeaderIp());
                        // 重置连接，并重新启动心跳流式传输
                        if (pulseClient.resetStub(pdInstruction.getLeaderIp(), pdPulse)) {
                            startHeartbeatStream(hbOnError);
                        }
                    }
                    return;
                }

                // 获取心跳响应中的分区心跳指令
                PartitionHeartbeatResponse instruct = content.getPartitionHeartbeatResponse();
                LOG.debug("Partition heartbeat receive instruction: {}", instruct);

                // 根据心跳指令创建分区对象
                Partition partition = new Partition(instruct.getPartition());

                // 遍历分区指令监听器列表
                for (PartitionInstructionListener event : partitionCommandListeners) {
                    // 判断是否包含变更分片指令
                    if (instruct.hasChangeShard()) {
                        event.onChangeShard(instruct.getId(), partition, instruct
                                                    .getChangeShard(),
                                            consumer);
                    }
                    // 判断是否包含拆分分区指令
                    if (instruct.hasSplitPartition()) {
                        event.onSplitPartition(instruct.getId(), partition,
                                               instruct.getSplitPartition(), consumer);
                    }
                    // 判断是否包含转移leader指令
                    if (instruct.hasTransferLeader()) {
                        event.onTransferLeader(instruct.getId(), partition,
                                               instruct.getTransferLeader(), consumer);
                    }
                    // 判断是否包含数据库压缩指令
                    if (instruct.hasDbCompaction()) {
                        event.onDbCompaction(instruct.getId(), partition,
                                             instruct.getDbCompaction(), consumer);
                    }

                    // 判断是否包含移动分区指令
                    if (instruct.hasMovePartition()) {
                        event.onMovePartition(instruct.getId(), partition,
                                              instruct.getMovePartition(), consumer);
                    }

                    // 判断是否包含清理分区指令
                    if (instruct.hasCleanPartition()) {
                        event.onCleanPartition(instruct.getId(), partition,
                                               instruct.getCleanPartition(),
                                               consumer);
                    }

                    // 判断是否包含分区键范围变更指令
                    if (instruct.hasKeyRange()) {
                        event.onPartitionKeyRangeChanged(instruct.getId(), partition,
                                                         instruct.getKeyRange(),
                                                         consumer);
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Partition heartbeat stream error. {}", throwable);
                // 重置连接，并调用错误回调函数
                pulseClient.resetStub(pdClient.getLeaderIp(), pdPulse);
                onError.accept(throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("Partition heartbeat stream complete");
            }
        });
        return true;
    }


    /**
     * 添加服务端消息监听
     *
     * @param listener
     * @return
     */
    @Override
    public boolean addPartitionInstructionListener(PartitionInstructionListener listener) {
        partitionCommandListeners.add(listener);
        return true;
    }

    @Override
    public boolean partitionHeartbeat(List<Metapb.PartitionStats> statsList) {
        for (Metapb.PartitionStats stats : statsList) {
            PartitionHeartbeatRequest.Builder request = PartitionHeartbeatRequest.newBuilder()
                                                                                 .setStates(stats);
            pdPulse.notifyServer(request);
        }
        return false;
    }

    @Override
    public boolean isLocalPartition(long storeId, int partitionId) {
        try {
            return !pdClient.queryPartitions(storeId, partitionId).isEmpty();
        } catch (PDException e) {
            log.error("isLocalPartition exception ", e);
        }
        return false;
    }

    @Override
    public Metapb.Graph getGraph(String graphName) throws PDException {
        return pdClient.getGraph(graphName);
    }

    @Override
    public void reportTask(MetaTask.Task task) throws PDException {
        pdClient.reportTask(task);
    }

    @Override
    public PDClient getPDClient() {
        return this.pdClient;
    }

    @Override
    public boolean updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        this.pdClient.updatePartitionLeader(graphName, partId, leaderStoreId);
        return true;
    }

    @Override
    public Store getStoreByID(Long storeId) {
        try {
            return new Store(pdClient.getStore(storeId));
        } catch (PDException e) {
            log.error("getStoreByID exception {}", e);
        }
        return null;
    }

    @Override
    public Metapb.ClusterStats getClusterStats() {
        try {
            return pdClient.getClusterStats();
        } catch (PDException e) {
            log.error("getClusterStats exception {}", e);
            return Metapb.ClusterStats.newBuilder()
                                      .setState(Metapb.ClusterState.Cluster_Fault).build();
        }
    }

    @Override
    public Metapb.ClusterStats storeHeartbeat(Store node) throws PDException {
        LOG.debug("storeHeartbeat node id: {}", node.getId());

        try {
            Metapb.StoreStats.Builder stats = HgMetricService.getInstance().getMetrics();
            LOG.debug("storeHeartbeat StoreStats: {}", stats);
            stats.setCores(node.getCores());
            return pdClient.storeHeartbeat(stats.build());

        } catch (PDException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Store {} report heartbeat exception: {}", node.getId(), e.toString());
        }

        return Metapb.ClusterStats.newBuilder()
                                  .setState(Metapb.ClusterState.Cluster_Fault).build();
    }


    private void handleCommonException(Exception e) {
    }

    @Override
    public GraphManager getGraphManager() {
        return graphManager;
    }

    @Override
    public void setGraphManager(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    @Override
    public void deleteShardGroup(int groupId) throws PDException {
        pdClient.deleteShardGroup(groupId);
    }

    @Override
    public Metapb.ShardGroup getShardGroup(int partitionId) {
        try {
            return pdClient.getShardGroup(partitionId);
        } catch (PDException e) {
            log.error("get shard group :{} from pd failed: {}", partitionId, e.getMessage());
        }
        return null;
    }

    @Override
    public void updateShardGroup(Metapb.ShardGroup shardGroup) throws PDException {
        pdClient.updateShardGroup(shardGroup);
    }
}
