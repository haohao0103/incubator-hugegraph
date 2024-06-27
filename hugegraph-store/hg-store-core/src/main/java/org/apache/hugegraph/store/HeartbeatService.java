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

package org.apache.hugegraph.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionRole;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.RaftRocksdbOptions;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.util.IpUtil;
import org.apache.hugegraph.store.util.Lifecycle;
import org.rocksdb.MemoryUsageType;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

/**
 * Register and heartbeat, Keep the system online
 */
@Slf4j
public class HeartbeatService implements Lifecycle<HgStoreEngineOptions>, PartitionStateListener {

    private static final int MAX_HEARTBEAT_RETRY_COUNT = 5;     // 心跳重试次数
    private static final int REGISTER_RETRY_INTERVAL = 1;   //注册重试时间间隔，单位秒
    private final HgStoreEngine storeEngine;
    private final List<HgStoreStateListener> stateListeners;
    private final Object partitionThreadLock = new Object();
    private final Object storeThreadLock = new Object();
    private HgStoreEngineOptions options;
    private PdProvider pdProvider;
    private Store storeInfo;
    private Metapb.ClusterStats clusterStats;
    private StoreMetadata storeMetadata;
    // 心跳失败次数
    private int heartbeatFailCount = 0;
    private int reportErrCount = 0;
    // 线程休眠时间
    private volatile int timerNextDelay = 1000;
    private boolean terminated = false;

    public HeartbeatService(HgStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        stateListeners = Collections.synchronizedList(new ArrayList());
    }

    @Override
    public boolean init(HgStoreEngineOptions opts) {
        // 设置选项
        this.options = opts;

        // 获取存储信息
        storeInfo = storeMetadata.getStore();
        if (storeInfo == null) {
            // 如果存储信息为空，则创建一个新的存储对象
            storeInfo = new Store();
        }

        // 设置存储地址
        storeInfo.setStoreAddress(options.getGrpcAddress());
        // 设置PD地址
        storeInfo.setPdAddress(options.getPdAddress());
        // 设置Raft地址
        storeInfo.setRaftAddress(options.getRaftAddress());
        // 设置存储状态为未知
        storeInfo.setState(Metapb.StoreState.Unknown);
        // 设置标签
        storeInfo.setLabels(options.getLabels());
        // 设置核心数
        storeInfo.setCores(Runtime.getRuntime().availableProcessors());
        // 设置部署路径
        storeInfo.setDeployPath(HeartbeatService.class.getResource("/").getPath());
        // 设置数据路径
        storeInfo.setDataPath(options.getDataPath());

        // 设置PD提供者
        this.pdProvider = options.getPdProvider();

        // 启动一个线程用于发送存储心跳
        new Thread(new Runnable() {
            @Override
            public void run() {
                doStoreHeartbeat();
            }
        }, "heartbeat").start();

        // 启动一个线程用于发送分区心跳
        new Thread(new Runnable() {
            @Override
            public void run() {
                doPartitionHeartbeat();
            }
        }, " partition-hb").start();

        // 返回初始化成功
        return true;
    }

    public HeartbeatService addStateListener(HgStoreStateListener stateListener) {
        stateListeners.add(stateListener);
        return this;
    }


    public Store getStoreInfo() {
        return storeInfo;
    }

    public void setStoreMetadata(StoreMetadata storeMetadata) {
        this.storeMetadata = storeMetadata;
    }

    // 集群是否准备就绪
    public boolean isClusterReady() {
        return clusterStats.getState() == Metapb.ClusterState.Cluster_OK;
    }

    /**
     * 服务状态有四种
     * 就绪，在线、离线、死亡（从集群排除）
     */
    protected void doStoreHeartbeat() {
        while (!terminated) {
            try {
                switch (storeInfo.getState()) {
                    case Unknown:
                    case Offline:
                        registerStore();
                        break;
                    case Up:
                        storeHeartbeat();
                        monitorMemory();
                        break;
                    case Tombstone:
                        break;

                }
                synchronized (storeThreadLock) {
                    storeThreadLock.wait(timerNextDelay);
                }
            } catch (Throwable e) {
                log.error("heartbeat error: ", e);
            }
        }
    }

    protected void doPartitionHeartbeat() {
        while (!terminated) {
            try {
                partitionHeartbeat();

            } catch (Exception e) {
                log.error("doPartitionHeartbeat error: ", e);
            }
            try {
                synchronized (partitionThreadLock) {
                    partitionThreadLock.wait(options.getPartitionHBInterval() * 1000L);
                }
            } catch (InterruptedException e) {
                log.error("doPartitionHeartbeat error: ", e);
            }
        }
    }

    protected void registerStore() {
        try {
            // 设置存储地址
            // 注册 store，初次注册 PD 产生 id，自动给 storeinfo 赋值
            this.storeInfo.setStoreAddress(IpUtil.getNearestAddress(options.getGrpcAddress()));
            // 设置 Raft 地址
            this.storeInfo.setRaftAddress(IpUtil.getNearestAddress(options.getRaftAddress()));

            // 注册 store 并获取 storeId
            long storeId = pdProvider.registerStore(this.storeInfo);
            if (storeId != 0) {
                // 如果 storeId 不为 0，说明注册成功
                storeInfo.setId(storeId);
                storeMetadata.save(storeInfo);
                // 获取集群状态
                this.clusterStats = pdProvider.getClusterStats();
                if (clusterStats.getState() == Metapb.ClusterState.Cluster_OK) {
                    // 如果集群状态正常，则设置下一次心跳间隔为配置中的值
                    timerNextDelay = options.getStoreHBInterval() * 1000;
                } else {
                    // 如果集群状态异常，则设置下一次心跳间隔为注册重试间隔
                    timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
                }
                // 打印日志，表示注册成功
                log.info("Register Store id= {} successfully. store = {}, clusterStats {}",
                         storeInfo.getId(), storeInfo, this.clusterStats);


                // 开始监听心跳流，并在连接关闭时执行相应操作
                pdProvider.startHeartbeatStream(error -> {
                    // 如果状态改变为离线，则更新下一次心跳间隔并唤醒心跳线程
                    onStateChanged(Metapb.StoreState.Offline);
                    timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
                    wakeupHeartbeatThread();
                    // 打印日志，表示连接已关闭，store 状态已更改为离线
                    log.error("Connection closed. The store state changes to {}",
                              Metapb.StoreState.Offline);
                });

                // 更新状态为 Up
                onStateChanged(Metapb.StoreState.Up);
            } else {
                // 如果 storeId 为 0，说明注册失败，设置下一次心跳间隔为注册重试间隔的一半
                timerNextDelay = REGISTER_RETRY_INTERVAL * 1000 / 2;
            }
        } catch (PDException e) {
            // 捕获异常
            int exceptCode = e.getErrorCode();
            if (exceptCode == Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE) {
                // 如果异常码为存储 ID 不存在
                log.error(
                        "The store ID {} does not match the PD. Check that the correct PD is " +
                        "connected, " +
                        "and then delete the store ID!!!",
                        storeInfo.getId());
                // 退出程序
                System.exit(-1);
            } else if (exceptCode == Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE) {
                // 如果异常码为存储已被删除
                log.error("The store ID {} has been removed, please delete all data and restart!",
                          storeInfo.getId());
                // 退出程序
                System.exit(-1);
            } else if (exceptCode == Pdpb.ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE) {
                // 如果异常码为禁止重复存储
                log.error(
                        "The store ID {} maybe duplicated, please check out store raft address " +
                        "and restart later!",
                        storeInfo.getId());
                // 退出程序
                System.exit(-1);
            }
        }
    }

    protected void storeHeartbeat() {
        if (log.isDebugEnabled()) {
            log.debug("storeHeartbeat ... ");
        }
        Metapb.ClusterStats clusterStats = null;
        try {
            clusterStats = pdProvider.storeHeartbeat(this.storeInfo);
        } catch (PDException e) {
            int exceptCode = e.getErrorCode();
            if (exceptCode == Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE) {
                log.error("The store ID {} does not match the PD. Check that the correct PD is " +
                          "connected, and then delete the store ID!!!", storeInfo.getId());
                System.exit(-1);
            } else if (exceptCode == Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE) {
                log.error("The store ID {} has been removed, please delete all data and restart!",
                          storeInfo.getId());
                System.exit(-1);
            }
        }
        if (clusterStats.getState().getNumber() >= Metapb.ClusterState.Cluster_Fault.getNumber()) {
            if (reportErrCount == 0) {
                log.info("The cluster is abnormal, {}", clusterStats);
            }
            reportErrCount = (++reportErrCount) % 30;
        }

        if (clusterStats.getState() == Metapb.ClusterState.Cluster_OK) {
            timerNextDelay = options.getStoreHBInterval() * 1000;
        } else {
            timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
        }

        if (clusterStats.getState() == Metapb.ClusterState.Cluster_Fault) {
            heartbeatFailCount++;
        } else {
            heartbeatFailCount = 0;
            this.clusterStats = clusterStats;
        }
        if (heartbeatFailCount > MAX_HEARTBEAT_RETRY_COUNT) {
            onStateChanged(Metapb.StoreState.Offline);
            timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
            this.clusterStats = clusterStats;
            log.error("Store heart beat failure. The store state changes to {}",
                      Metapb.StoreState.Offline);
        }
    }

    protected synchronized void onStateChanged(Metapb.StoreState newState) {
        Utils.runInThread(() -> {
            Metapb.StoreState oldState = this.storeInfo.getState();
            this.storeInfo.setState(newState);
            stateListeners.forEach((e) ->
                                           e.stateChanged(this.storeInfo, oldState, newState));
        });
    }


    protected void partitionHeartbeat() {
        if (storeEngine == null) {
            return;
        }

        List<PartitionEngine> partitions = storeEngine.getLeaderPartition();
        final List<Metapb.PartitionStats> statsList = new ArrayList<>(partitions.size());

        Metapb.Shard localLeader = Metapb.Shard.newBuilder()
                                               .setStoreId(
                                                       storeEngine.getPartitionManager().getStore()
                                                                  .getId())
                                               .setRole(Metapb.ShardRole.Leader)
                                               .build();
        // 获取各个 shard 信息。
        for (PartitionEngine partition : partitions) {
            Metapb.PartitionStats.Builder stats = Metapb.PartitionStats.newBuilder();
            stats.setId(partition.getGroupId());
            stats.addAllGraphName(partition.getPartitions().keySet());
            stats.setLeaderTerm(partition.getLeaderTerm());
            stats.setConfVer(partition.getShardGroup().getConfVersion());
            stats.setLeader(localLeader);

            stats.addAllShard(partition.getShardGroup().getMetaPbShard());

            // shard 状态
            List<Metapb.ShardStats> shardStats = new ArrayList<>();
            Map<Long, PeerId> aliveShards = partition.getAlivePeers();
            // 统计 shard 状态
            partition.getShardGroup().getShards().forEach(shard -> {
                Metapb.ShardState state = Metapb.ShardState.SState_Normal;
                if (! aliveShards.containsKey(shard.getStoreId())) {
                    state = Metapb.ShardState.SState_Offline;
                }

                shardStats.add(Metapb.ShardStats.newBuilder()
                                                .setStoreId(shard.getStoreId())
                                                .setRole(shard.getRole())
                                                .setState(state).build());
            });
            stats.addAllShardStats(shardStats);
            stats.setTimestamp(System.currentTimeMillis());

            statsList.add(stats.build());
        }
        // 发送心跳
        if (statsList.size() > 0) {
            pdProvider.partitionHeartbeat(statsList);
        }

    }

    public void monitorMemory() {

        try {
            Map<MemoryUsageType, Long> mems =
                    storeEngine.getBusinessHandler().getApproximateMemoryUsageByType(null);

            if (mems.get(MemoryUsageType.kCacheTotal) >
                RaftRocksdbOptions.getWriteCacheCapacity() * 0.9 &&
                mems.get(MemoryUsageType.kMemTableUnFlushed) >
                RaftRocksdbOptions.getWriteCacheCapacity() * 0.1) {
                // storeEngine.getBusinessHandler().flushAll();
                log.warn("Less memory, start flush dbs, {}", mems);
            }
        } catch (Exception e) {
            log.error("MonitorMemory exception {}", e);
        }
    }

    @Override
    public void shutdown() {
        log.info("HeartbeatService shutdown");
        terminated = true;
        synchronized (partitionThreadLock) {
            partitionThreadLock.notify();
        }
    }

    @Override
    public void partitionRoleChanged(Partition partition, PartitionRole newRole) {
        if (newRole == PartitionRole.LEADER) {
            // leader 发生改变，激活心跳
            synchronized (partitionThreadLock) {
                partitionThreadLock.notifyAll();
            }
        }
    }

    @Override
    public void partitionShardChanged(Partition partition, List<Metapb.Shard> oldShards,
                                      List<Metapb.Shard> newShards) {
        if (partition.isLeader()) {
            synchronized (partitionThreadLock) {
                partitionThreadLock.notifyAll();
            }
        }
    }

    private void wakeupHeartbeatThread() {
        synchronized (storeThreadLock) {
            storeThreadLock.notifyAll();
        }
    }
}
