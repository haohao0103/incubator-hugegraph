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

package org.apache.hugegraph.pd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.GraphMode;
import org.apache.hugegraph.pd.grpc.Metapb.GraphModeReason;
import org.apache.hugegraph.pd.grpc.Metapb.GraphState;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.pulse.ConfChangeType;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;


/**
 * HgStore注册、保活管理类
 */
@Slf4j
public class StoreNodeService {

    private static final Long STORE_HEART_BEAT_INTERVAL = 30000L;
    private static final String graphSpaceConfPrefix = "HUGEGRAPH/hg/GRAPHSPACE/CONF/";
    // Store状态监听
    private final List<StoreStatusListener> statusListeners;
    private final List<ShardGroupStatusListener> shardGroupStatusListeners;
    private final StoreInfoMeta storeInfoMeta;
    private final TaskInfoMeta taskInfoMeta;
    private final Random random = new Random(System.currentTimeMillis());
    private final KvService kvService;
    private final ConfigService configService;
    private final PDConfig pdConfig;
    private PartitionService partitionService;
    private final Runnable quotaChecker = () -> {
        try {
            getQuota();
        } catch (Exception e) {
            log.error(
                    "obtaining and sending graph space quota information with error: ",
                    e);
        }
    };
    private Metapb.ClusterStats clusterStats;

    public StoreNodeService(PDConfig config) {
        // 将传入的配置赋值给成员变量pdConfig
        this.pdConfig = config;

        // 调用MetadataFactory的newStoreInfoMeta方法创建StoreInfoMeta对象，并将配置作为参数传入
        storeInfoMeta = MetadataFactory.newStoreInfoMeta(pdConfig);

        // 调用MetadataFactory的newTaskInfoMeta方法创建TaskInfoMeta对象，并将配置作为参数传入
        taskInfoMeta = MetadataFactory.newTaskInfoMeta(pdConfig);

        // 创建一个线程安全的ArrayList，并将其赋值给shardGroupStatusListeners成员变量
        shardGroupStatusListeners = Collections.synchronizedList(new ArrayList<>());

        // 创建一个线程安全的ArrayList，并将其赋值给statusListeners成员变量
        statusListeners = Collections.synchronizedList(new ArrayList<StoreStatusListener>());

        // 创建一个Metapb.ClusterStats对象，并设置其状态为Cluster_Not_Ready，时间戳为当前时间戳
        clusterStats = Metapb.ClusterStats.newBuilder()
                                          .setState(Metapb.ClusterState.Cluster_Not_Ready)
                                          .setTimestamp(System.currentTimeMillis())
                                          .build();

        // 创建一个KvService对象，并将配置作为参数传入
        kvService = new KvService(pdConfig);

        // 创建一个ConfigService对象，并将配置作为参数传入
        configService = new ConfigService(pdConfig);
    }

    public void init(PartitionService partitionService) {
        this.partitionService = partitionService;
        partitionService.addStatusListener(new PartitionStatusListener() {
            @Override
            public void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
                if (old != null && old.getState() != partition.getState()) {
                    // 状态改变，重置集群状态
                    try {
                        List<Metapb.Partition> partitions =
                                partitionService.getPartitionById(partition.getId());
                        Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;
                        for (Metapb.Partition pt : partitions) {
                            if (pt.getState().getNumber() > state.getNumber()) {
                                state = pt.getState();
                            }
                        }
                        updateShardGroupState(partition.getId(), state);

                        for (Metapb.ShardGroup group : getShardGroups()) {
                            if (group.getState().getNumber() > state.getNumber()) {
                                state = group.getState();
                            }
                        }
                        updateClusterStatus(state);
                    } catch (PDException e) {
                        log.error("onPartitionChanged exception: ", e);
                    }
                }
            }

            @Override
            public void onPartitionRemoved(Metapb.Partition partition) {

            }
        });
    }

    /**
     * 集群是否准备就绪
     *
     * @return
     */
    public boolean isOK() {
        return this.clusterStats.getState().getNumber() <
               Metapb.ClusterState.Cluster_Offline.getNumber();
    }

    /**
     * Store注册，记录Store的ip地址，首次注册需要生成store_ID
     *
     * @param store
     */
    public Metapb.Store register(Metapb.Store store) throws PDException {
        if (store.getId() == 0) {
            // 初始注册，生成新id，保证Id不重复。
            store = newStoreNode(store);
        }

        if (!storeInfoMeta.storeExists(store.getId())) {
            log.error("Store id {} does not belong to this PD, address = {}", store.getId(),
                      store.getAddress());
            // storeId不存在，抛出异常
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %d doest not exist.", store.getId()));
        }

        // 如果store状态为Tombstone拒绝注册。
        Metapb.Store lastStore = storeInfoMeta.getStore(store.getId());
        if (lastStore.getState() == Metapb.StoreState.Tombstone) {
            log.error("Store id {} has been removed, Please reinitialize, address = {}",
                      store.getId(), store.getAddress());
            // storeId不存在，抛出异常
            throw new PDException(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                  String.format("Store id %d has been removed. %s", store.getId(),
                                                store.getAddress()));
        }

        // offline或者up，或者在初始激活列表中，自动上线
        Metapb.StoreState storeState = lastStore.getState();
        if (storeState == Metapb.StoreState.Offline || storeState == Metapb.StoreState.Up
            || inInitialStoreList(store)) {
            storeState = Metapb.StoreState.Up;
        } else {
            storeState = Metapb.StoreState.Pending;
        }

        store = Metapb.Store.newBuilder(lastStore)
                            .setAddress(store.getAddress())
                            .setRaftAddress(store.getRaftAddress())
                            .setDataVersion(store.getDataVersion())
                            .setDeployPath(store.getDeployPath())
                            .setVersion(store.getVersion())
                            .setDataPath(store.getDataPath())
                            .setState(storeState).setCores(store.getCores())
                            .clearLabels().addAllLabels(store.getLabelsList())
                            .setLastHeartbeat(System.currentTimeMillis()).build();

        long current = System.currentTimeMillis();
        boolean raftChanged = false;
        // 上线状态的Raft Address 发生了变更
        if (!Objects.equals(lastStore.getRaftAddress(), store.getRaftAddress()) &&
            storeState == Metapb.StoreState.Up) {
            // 时间间隔太短，而且raft有变更，则认为是无效的store
            if (current - lastStore.getLastHeartbeat() < STORE_HEART_BEAT_INTERVAL * 0.8) {
                throw new PDException(Pdpb.ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE,
                                      String.format("Store id %d may be duplicate. addr: %s",
                                                    store.getId(), store.getAddress()));
            } else if (current - lastStore.getLastHeartbeat() > STORE_HEART_BEAT_INTERVAL * 1.2) {
                // 认为发生了变更
                raftChanged = true;
            } else {
                // 等待下次注册
                return Metapb.Store.newBuilder(store).setId(0L).build();
            }
        }

        // 存储store信息
        storeInfoMeta.updateStore(store);
        if (storeState == Metapb.StoreState.Up) {
            // 更新store 活跃状态
            storeInfoMeta.keepStoreAlive(store);
            onStoreStatusChanged(store, Metapb.StoreState.Offline, Metapb.StoreState.Up);
            checkStoreStatus();
        }

        // 等store信息保存后，再发送变更
        if (raftChanged) {
            onStoreRaftAddressChanged(store);
        }

        log.info("Store register, id = {} {}", store.getId(), store);
        return store;
    }

    private boolean inInitialStoreList(Metapb.Store store) {
        return this.pdConfig.getInitialStoreMap().containsKey(store.getAddress());
    }

    /**
     * 产生一个新的store对象
     *
     * @param store
     * @return
     * @throws PDException
     */
    private synchronized Metapb.Store newStoreNode(Metapb.Store store) throws PDException {
        long id = random.nextLong() & Long.MAX_VALUE;
        while (id == 0 || storeInfoMeta.storeExists(id)) {
            id = random.nextLong() & Long.MAX_VALUE;
        }
        store = Metapb.Store.newBuilder(store)
                            .setId(id)
                            .setState(Metapb.StoreState.Pending)
                            .setStartTimestamp(System.currentTimeMillis()).build();
        storeInfoMeta.updateStore(store);
        return store;
    }

    /**
     * 根据store_id返回Store信息
     *
     * @param id
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long id) throws PDException {
        Metapb.Store store = storeInfoMeta.getStore(id);
        if (store == null) {
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %x doest not exist.", id));
        }
        return store;
    }

    /**
     * 更新Store信息，检测Store状态的变化，通知到Hugestore
     */
    public synchronized Metapb.Store updateStore(Metapb.Store store) throws PDException {
        log.info("updateStore storeId: {}, address: {}, state: {}", store.getId(),
                 store.getAddress(), store.getState());
        Metapb.Store lastStore = storeInfoMeta.getStore(store.getId());
        if (lastStore == null) {
            return null;
        }
        Metapb.Store.Builder builder =
                Metapb.Store.newBuilder(lastStore).clearLabels().clearStats();
        store = builder.mergeFrom(store).build();
        if (store.getState() == Metapb.StoreState.Tombstone) {
            List<Metapb.Store> activeStores = getStores();
            if (lastStore.getState() == Metapb.StoreState.Up
                && activeStores.size() - 1 < pdConfig.getMinStoreCount()) {
                throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                      "The number of active stores is less then " +
                                      pdConfig.getMinStoreCount());
            }
        }

        storeInfoMeta.updateStore(store);
        if (store.getState() != Metapb.StoreState.Unknown &&
            store.getState() != lastStore.getState()) {
            // 如果希望将store下线
            if (store.getState() == Metapb.StoreState.Exiting) {
                if (lastStore.getState() == Metapb.StoreState.Exiting) {
                    //如果已经是下线中的状态，则不作进一步处理
                    return lastStore;
                }

                List<Metapb.Store> activeStores = this.getActiveStores();
                Map<Long, Metapb.Store> storeMap = new HashMap<>();
                activeStores.forEach(s -> {
                    storeMap.put(s.getId(), s);
                });
                //如果store已经离线，直接从活跃中删除，如果store在线，暂时不从活跃中删除，等把状态置成Tombstone的时候再删除
                if (!storeMap.containsKey(store.getId())) {
                    log.info("updateStore removeActiveStores store {}", store.getId());
                    storeInfoMeta.removeActiveStore(store);
                }
                storeTurnoff(store);
            } else if (store.getState() == Metapb.StoreState.Offline) {  //监控到store已经离线，从活跃中删除
                storeInfoMeta.removeActiveStore(store);
            } else if (store.getState() == Metapb.StoreState.Tombstone) {
                // 状态发生改变，Store关机，修改shardGroup，进行副本迁移
                log.info("updateStore removeActiveStores store {}", store.getId());
                storeInfoMeta.removeActiveStore(store);
                // 存储下线
                storeTurnoff(store);
            } else if (store.getState() == Metapb.StoreState.Up) {
                storeInfoMeta.keepStoreAlive(store);
                checkStoreStatus();
            }
            onStoreStatusChanged(lastStore, lastStore.getState(), store.getState());
        }
        return store;
    }

    /**
     * store被关机，重新分配shardGroup的shard
     *
     * @param store
     * @throws PDException
     */
    public synchronized void storeTurnoff(Metapb.Store store) throws PDException {
        // 遍历ShardGroup，重新分配shard
        for (Metapb.ShardGroup group : getShardGroupsByStore(store.getId())) {
            Metapb.ShardGroup.Builder builder = Metapb.ShardGroup.newBuilder(group);
            builder.clearShards();
            group.getShardsList().forEach(shard -> {
                if (shard.getStoreId() != store.getId()) {
                    builder.addShards(shard);
                }
            });
            reallocShards(builder.build());
        }
    }

    /**
     * 根据图名返回stores信息，如果graphName为空，返回所有store信息
     *
     * @throws PDException
     */
    public List<Metapb.Store> getStores() throws PDException {
        return storeInfoMeta.getStores(null);
    }

    public List<Metapb.Store> getStores(String graphName) throws PDException {
        return storeInfoMeta.getStores(graphName);
    }

    public List<Metapb.Store> getStoreStatus(boolean isActive) throws PDException {
        return storeInfoMeta.getStoreStatus(isActive);
    }

    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
        return storeInfoMeta.getShardGroups();
    }

    public Metapb.ShardGroup getShardGroup(int groupId) throws PDException {
        return storeInfoMeta.getShardGroup(groupId);
    }

    public List<Metapb.Shard> getShardList(int groupId) throws PDException {
        var shardGroup = getShardGroup(groupId);
        if (shardGroup != null) {
            return shardGroup.getShardsList();
        }
        return new ArrayList<>();
    }

    public List<Metapb.ShardGroup> getShardGroupsByStore(long storeId) throws PDException {
        List<Metapb.ShardGroup> shardGroups = new ArrayList<>();
        storeInfoMeta.getShardGroups().forEach(shardGroup -> {
            shardGroup.getShardsList().forEach(shard -> {
                if (shard.getStoreId() == storeId) {
                    shardGroups.add(shardGroup);
                }
            });
        });
        return shardGroups;
    }

    /**
     * 返回活跃的store
     *
     * @param graphName
     * @return
     * @throws PDException
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        return storeInfoMeta.getActiveStores(graphName);
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        return storeInfoMeta.getActiveStores();
    }

    public List<Metapb.Store> getTombStores() throws PDException {
        List<Metapb.Store> stores = new ArrayList<>();
        for (Metapb.Store store : this.getStores()) {
            if (store.getState() == Metapb.StoreState.Tombstone) {
                stores.add(store);
            }
        }
        return stores;
    }

    public long removeStore(Long storeId) throws PDException {
        return storeInfoMeta.removeStore(storeId);
    }

    /**
     * 给partition分配store，根据图的配置，决定分配几个peer
     * 分配完所有的shards，保存ShardGroup对象（store不变动，只执行一次）
     */
    public synchronized List<Metapb.Shard> allocShards(Metapb.Graph graph, int partId) throws
                                                                                       PDException {
        // 多图共用raft分组，因此分配shard只依赖partitionId.
        // 图根据数据大小可以设置分区的数量，但总数不能超过raft分组数量
        if (storeInfoMeta.getShardGroup(partId) == null) {
            // 获取活跃的store key
            // 根据 partionID计算store
            List<Metapb.Store> stores = storeInfoMeta.getActiveStores();

            if (stores.size() == 0) {
                throw new PDException(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE,
                                      "There is no any online store");
            }

            if (stores.size() < pdConfig.getMinStoreCount()) {
                throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                      "The number of active stores is less then " +
                                      pdConfig.getMinStoreCount());
            }

            int shardCount = pdConfig.getPartition().getShardCount();
            shardCount = Math.min(shardCount, stores.size());
            //两个shard无法选出leader
            // 不能为0

            if (shardCount == 2 || shardCount < 1) {
                shardCount = 1;
            }

            // 一次创建完所有的ShardGroup，保证初始的groupID有序，方便人工阅读
            for (int groupId = 0; groupId < pdConfig.getConfigService().getPartitionCount();
                 groupId++) {
                int storeIdx = groupId % stores.size();  //store分配规则，简化为取模
                List<Metapb.Shard> shards = new ArrayList<>();
                for (int i = 0; i < shardCount; i++) {
                    Metapb.Shard shard =
                            Metapb.Shard.newBuilder().setStoreId(stores.get(storeIdx).getId())
                                        .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                                 Metapb.ShardRole.Follower) //
                                        .build();
                    shards.add(shard);
                    storeIdx = (storeIdx + 1) >= stores.size() ? 0 : ++storeIdx; // 顺序选择
                }

                Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder()
                                                           .setId(groupId)
                                                           .setState(
                                                                   Metapb.PartitionState.PState_Normal)
                                                           .addAllShards(shards).build();

                // new group
                storeInfoMeta.updateShardGroup(group);
                partitionService.updateShardGroupCache(group);
                onShardGroupStatusChanged(group, group);
                log.info("alloc shard group: id {}", groupId);
            }
        }
        return storeInfoMeta.getShardGroup(partId).getShardsList();
    }

    /**
     * 根据graph的shard_count，重新分配shard
     * 发送变更change shard指令
     */
    public synchronized List<Metapb.Shard> reallocShards(Metapb.ShardGroup shardGroup) throws
                                                                                       PDException {
        List<Metapb.Store> stores = storeInfoMeta.getActiveStores();

        if (stores.size() == 0) {
            throw new PDException(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE,
                                  "There is no any online store");
        }

        if (stores.size() < pdConfig.getMinStoreCount()) {
            throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                  "The number of active stores is less then " +
                                  pdConfig.getMinStoreCount());
        }

        int shardCount = pdConfig.getPartition().getShardCount();
        shardCount = Math.min(shardCount, stores.size());
        if (shardCount == 2 || shardCount < 1) {
            // 两个shard无法选出leader
            // 不能为0
            shardCount = 1;
        }

        List<Metapb.Shard> shards = new ArrayList<>();
        shards.addAll(shardGroup.getShardsList());

        if (shardCount > shards.size()) {
            // 需要增加shard
            log.info("reallocShards ShardGroup {}, add shards from {} to {}",
                     shardGroup.getId(), shards.size(), shardCount);
            int storeIdx = shardGroup.getId() % stores.size();  //store分配规则，简化为取模
            for (int addCount = shardCount - shards.size(); addCount > 0; ) {
                // 检查是否已经存在
                if (!isStoreInShards(shards, stores.get(storeIdx).getId())) {
                    Metapb.Shard shard = Metapb.Shard.newBuilder()
                                                     .setStoreId(stores.get(storeIdx).getId())
                                                     .build();
                    shards.add(shard);
                    addCount--;
                }
                storeIdx = (storeIdx + 1) >= stores.size() ? 0 : ++storeIdx; // 顺序选择
            }
        } else if (shardCount < shards.size()) {
            // 需要减shard
            log.info("reallocShards ShardGroup {}, remove shards from {} to {}",
                     shardGroup.getId(), shards.size(), shardCount);

            int subCount = shards.size() - shardCount;
            Iterator<Metapb.Shard> iterator = shards.iterator();
            while (iterator.hasNext() && subCount > 0) {
                if (iterator.next().getRole() != Metapb.ShardRole.Leader) {
                    iterator.remove();
                    subCount--;
                }
            }
        } else {
            return shards;
        }

        Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder(shardGroup)
                                                   .clearShards()
                                                   .addAllShards(shards).build();
        storeInfoMeta.updateShardGroup(group);
        partitionService.updateShardGroupCache(group);
        // change shard group
        onShardGroupStatusChanged(shardGroup, group);

        var partitions = partitionService.getPartitionById(shardGroup.getId());
        if (partitions.size() > 0) {
            // send one message, change shard is regardless with partition/graph
            partitionService.fireChangeShard(partitions.get(0), shards,
                                             ConfChangeType.CONF_CHANGE_TYPE_ADJUST);
        }

        log.info("reallocShards ShardGroup {}, shards: {}", group.getId(), group.getShardsList());
        return shards;
    }

    /**
     * 根据partition的数量，分配group shard
     *
     * @param groups list of (partition id, count)
     * @return total groups
     */
    public synchronized int splitShardGroups(List<KVPair<Integer, Integer>> groups) throws
                                                                                    PDException {
        int sum = groups.stream().map(pair -> pair.getValue()).reduce(0, Integer::sum);
        // shard group 太大
        if (sum > getActiveStores().size() * pdConfig.getPartition().getMaxShardsPerStore()) {
            throw new PDException(Pdpb.ErrorType.Too_Many_Partitions_Per_Store_VALUE,
                                  "can't satisfy target shard group count");
        }

        partitionService.splitPartition(groups);

        return sum;
    }

    /**
     * 分配shard group，为分裂做准备
     *
     * @return true
     * @throws PDException
     */
    private boolean isStoreInShards(List<Metapb.Shard> shards, long storeId) {
        AtomicBoolean exist = new AtomicBoolean(false);
        shards.forEach(s -> {
            if (s.getStoreId() == storeId) {
                exist.set(true);
            }
        });
        return exist.get();
    }

    /**
     * update shard group and cache.
     * send shard group change message.
     *
     * @param groupId     : shard group
     * @param shards      : shard lists
     * @param version:    term version, ignored if less than 0
     * @param confVersion : conf version, ignored if less than 0
     * @return
     */
    public synchronized Metapb.ShardGroup updateShardGroup(int groupId, List<Metapb.Shard> shards,
                                                           long version, long confVersion) throws
                                                                                           PDException {
        Metapb.ShardGroup group = this.storeInfoMeta.getShardGroup(groupId);

        if (group == null) {
            return null;
        }

        var builder = Metapb.ShardGroup.newBuilder(group);
        if (version >= 0) {
            builder.setVersion(version);
        }

        if (confVersion >= 0) {
            builder.setConfVer(confVersion);
        }

        var newGroup = builder.clearShards().addAllShards(shards).build();

        storeInfoMeta.updateShardGroup(newGroup);
        partitionService.updateShardGroupCache(newGroup);
        onShardGroupStatusChanged(group, newGroup);
        log.info("Raft {} updateShardGroup {}", groupId, newGroup);
        return group;
    }

    /**
     * 通知 store 进行shard group的重建操作
     *
     * @param groupId raft group id
     * @param shards  shard list: 如果为空，则删除对应的partition engine
     */
    public void shardGroupOp(int groupId, List<Metapb.Shard> shards) throws PDException {

        var shardGroup = getShardGroup(groupId);

        if (shardGroup == null) {
            return;
        }

        var newGroup = shardGroup.toBuilder().clearShards().addAllShards(shards).build();
        if (shards.size() == 0) {
            var partitions = partitionService.getPartitionById(groupId);
            for (var partition : partitions) {
                partitionService.removePartition(partition.getGraphName(), groupId);
            }
            deleteShardGroup(groupId);
        }

        onShardGroupOp(newGroup);
    }

    /**
     * 删除 shard group
     *
     * @param groupId shard group id
     */
    public synchronized void deleteShardGroup(int groupId) throws PDException {
        Metapb.ShardGroup group = this.storeInfoMeta.getShardGroup(groupId);
        if (group != null) {
            storeInfoMeta.deleteShardGroup(groupId);
        }

        onShardGroupStatusChanged(group, null);

        // 修正store的分区数. (分区合并导致)
        var shardGroups = getShardGroups();
        if (shardGroups != null) {
            var count1 = pdConfig.getConfigService().getPDConfig().getPartitionCount();
            var maxGroupId =
                    getShardGroups().stream().map(Metapb.ShardGroup::getId).max(Integer::compareTo);
            if (maxGroupId.get() < count1) {
                pdConfig.getConfigService().setPartitionCount(maxGroupId.get() + 1);
            }
        }
    }

    public synchronized void updateShardGroupState(int groupId, Metapb.PartitionState state) throws
                                                                                             PDException {
        Metapb.ShardGroup shardGroup = storeInfoMeta.getShardGroup(groupId)
                                                    .toBuilder()
                                                    .setState(state).build();
        storeInfoMeta.updateShardGroup(shardGroup);
        partitionService.updateShardGroupCache(shardGroup);
    }

    /**
     * 接收Store的心跳
     *
     * @param storeStats
     * @throws PDException
     */
    /**
     * 心跳函数，用于更新Store的状态信息，并返回集群状态信息。
     *
     * @param storeStats 存储节点的状态信息
     * @return 集群状态信息
     * @throws PDException 当Store不存在或者已经被移除时，抛出异常
     */
    public Metapb.ClusterStats heartBeat(Metapb.StoreStats storeStats) throws PDException {
        this.storeInfoMeta.updateStoreStats(storeStats);
        Metapb.Store lastStore = this.getStore(storeStats.getStoreId());
        if (lastStore == null) {
            //store不存在
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %d does not exist.",
                                                storeStats.getStoreId()));
        }
        if (lastStore.getState() == Metapb.StoreState.Tombstone) {
            throw new PDException(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                  String.format(
                                          "Store id %d is useless since it's state is Tombstone",
                                          storeStats.getStoreId()));
        }
        Metapb.Store nowStore;
        // 如果正在做store下线操作
        if (lastStore.getState() == Metapb.StoreState.Exiting) {
            List<Metapb.Store> activeStores = this.getActiveStores();
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                storeMap.put(store.getId(), store);
            });
            // 下线的store的分区为0，说明已经迁移完毕，可以下线，如果非0，则迁移还在进行，需要等待
            if (storeStats.getPartitionCount() > 0 &&
                storeMap.containsKey(storeStats.getStoreId())) {
                nowStore = Metapb.Store.newBuilder(lastStore)
                                       .setStats(storeStats)
                                       .setLastHeartbeat(System.currentTimeMillis())
                                       .setState(Metapb.StoreState.Exiting).build();
                this.storeInfoMeta.updateStore(nowStore);
                return this.clusterStats;
            } else {
                nowStore = Metapb.Store.newBuilder(lastStore)
                                       .setStats(storeStats)
                                       .setLastHeartbeat(System.currentTimeMillis())
                                       .setState(Metapb.StoreState.Tombstone).build();
                this.storeInfoMeta.updateStore(nowStore);
                storeInfoMeta.removeActiveStore(nowStore);
                return this.clusterStats;
            }
        }

        if (lastStore.getState() == Metapb.StoreState.Pending) {
            nowStore = Metapb.Store.newBuilder(lastStore)
                                   .setStats(storeStats)
                                   .setLastHeartbeat(System.currentTimeMillis())
                                   .setState(Metapb.StoreState.Pending).build();
            this.storeInfoMeta.updateStore(nowStore);
            return this.clusterStats;
        } else {
            if (lastStore.getState() == Metapb.StoreState.Offline) {
                this.updateStore(
                        Metapb.Store.newBuilder(lastStore).setState(Metapb.StoreState.Up).build());
            }
            nowStore = Metapb.Store.newBuilder(lastStore)
                                   .setState(Metapb.StoreState.Up)
                                   .setStats(storeStats)
                                   .setLastHeartbeat(System.currentTimeMillis()).build();
            this.storeInfoMeta.updateStore(nowStore);
            this.storeInfoMeta.keepStoreAlive(nowStore);
            this.checkStoreStatus();
            return this.clusterStats;
        }
    }

    public synchronized Metapb.ClusterStats updateClusterStatus(Metapb.ClusterState state) {
        this.clusterStats = clusterStats.toBuilder().setState(state).build();
        return this.clusterStats;
    }

    public Metapb.ClusterStats updateClusterStatus(Metapb.PartitionState state) {
        Metapb.ClusterState cstate = Metapb.ClusterState.Cluster_OK;
        switch (state) {
            case PState_Normal:
                cstate = Metapb.ClusterState.Cluster_OK;
                break;
            case PState_Warn:
                cstate = Metapb.ClusterState.Cluster_Warn;
                break;
            case PState_Fault:
                cstate = Metapb.ClusterState.Cluster_Fault;
                break;
            case PState_Offline:
                cstate = Metapb.ClusterState.Cluster_Offline;
                break;
        }
        return updateClusterStatus(cstate);
    }

    public Metapb.ClusterStats getClusterStats() {
        return this.clusterStats;
    }

    /**
     * 检查集群健康状态
     * 活跃机器数是否大于最小阈值
     * 分区shard在线数已否过半     *
     */
    public synchronized void checkStoreStatus() {
        Metapb.ClusterStats.Builder builder = Metapb.ClusterStats.newBuilder()
                                                                 .setState(
                                                                         Metapb.ClusterState.Cluster_OK);
        try {
            List<Metapb.Store> activeStores = this.getActiveStores();
            if (activeStores.size() < pdConfig.getMinStoreCount()) {
                builder.setState(Metapb.ClusterState.Cluster_Not_Ready);
                builder.setMessage("The number of active stores is " + activeStores.size()
                                   + ", less than pd.min-store-count:" +
                                   pdConfig.getMinStoreCount());
            }
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                storeMap.put(store.getId(), store);
            });

            if (builder.getState() == Metapb.ClusterState.Cluster_OK) {
                // 检查每个分区的在线shard数量是否大于半数
                for (Metapb.ShardGroup group : this.getShardGroups()) {
                    int count = 0;
                    for (Metapb.Shard shard : group.getShardsList()) {
                        count += storeMap.containsKey(shard.getStoreId()) ? 1 : 0;
                    }
                    if (count * 2 < group.getShardsList().size()) {
                        builder.setState(Metapb.ClusterState.Cluster_Not_Ready);
                        builder.setMessage(
                                "Less than half of active shard, partitionId is " + group.getId());
                        break;
                    }
                }
            }

        } catch (PDException e) {
            log.error("StoreNodeService updateClusterStatus exception {}", e);
        }
        this.clusterStats = builder.setTimestamp(System.currentTimeMillis()).build();
        if (this.clusterStats.getState() != Metapb.ClusterState.Cluster_OK) {
            log.error("The cluster is not ready, {}", this.clusterStats);
        }
    }

    public void addStatusListener(StoreStatusListener listener) {
        statusListeners.add(listener);
    }

    protected void onStoreRaftAddressChanged(Metapb.Store store) {
        log.info("onStoreRaftAddressChanged storeId = {}, new raft addr:", store.getId(),
                 store.getRaftAddress());
        statusListeners.forEach(e -> {
            e.onStoreRaftChanged(store);
        });
    }

    public void addShardGroupStatusListener(ShardGroupStatusListener listener) {
        shardGroupStatusListeners.add(listener);
    }

    protected void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                                        Metapb.StoreState stats) {
        log.info("onStoreStatusChanged storeId = {} from {} to {}", store.getId(), old, stats);
        statusListeners.forEach(e -> {
            e.onStoreStatusChanged(store, old, stats);
        });
    }

    protected void onShardGroupStatusChanged(Metapb.ShardGroup group, Metapb.ShardGroup newGroup) {
        log.info("onShardGroupStatusChanged, groupId: {}, from {} to {}", group.getId(), group,
                 newGroup);
        shardGroupStatusListeners.forEach(e -> e.onShardListChanged(group, newGroup));
    }

    protected void onShardGroupOp(Metapb.ShardGroup shardGroup) {
        log.info("onShardGroupOp, group id: {}, shard group:{}", shardGroup.getId(), shardGroup);
        shardGroupStatusListeners.forEach(e -> e.onShardListOp(shardGroup));
    }

    /**
     * 检查当前store是否可下线
     * 活跃机器数小于等于最小阈值，不可下线
     * 分区shard在线数不超过半数， 不可下线
     */
    public boolean checkStoreCanOffline(Metapb.Store currentStore) {
        try {
            long currentStoreId = currentStore.getId();
            List<Metapb.Store> activeStores = this.getActiveStores();
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                if (store.getId() != currentStoreId) {
                    storeMap.put(store.getId(), store);
                }
            });

            if (storeMap.size() < pdConfig.getMinStoreCount()) {
                return false;
            }

            // 检查每个分区的在线shard数量是否大于半数
            for (Metapb.ShardGroup group : this.getShardGroups()) {
                int count = 0;
                for (Metapb.Shard shard : group.getShardsList()) {
                    long storeId = shard.getStoreId();
                    count += storeMap.containsKey(storeId) ? 1 : 0;
                }
                if (count * 2 < group.getShardsList().size()) {
                    return false;
                }
            }
        } catch (PDException e) {
            log.error("StoreNodeService checkStoreCanOffline exception {}", e);
            return false;
        }

        return true;
    }

    /**
     * 对store上的对rocksdb进行compaction
     *
     * @param groupId
     * @param tableName
     * @return
     */
    public synchronized void shardGroupsDbCompaction(int groupId, String tableName) throws
                                                                                    PDException {

        // 通知所有的store，对rocksdb进行compaction
        partitionService.fireDbCompaction(groupId, tableName);
        // TODO 异常怎么处理？
    }

    public Map getQuota() throws PDException {
        List<Metapb.Graph> graphs = partitionService.getGraphs();
        String delimiter = String.valueOf(MetadataKeyHelper.DELIMITER);
        HashMap<String, Long> storages = new HashMap<>();
        for (Metapb.Graph g : graphs) {
            String graphName = g.getGraphName();
            String[] splits = graphName.split(delimiter);
            if (!graphName.endsWith("/g") || splits.length < 2) {
                continue;
            }
            String graphSpace = splits[0];
            storages.putIfAbsent(graphSpace, 0L);
            List<Metapb.Store> stores = getStores(graphName);
            long dataSize = 0;
            for (Metapb.Store store : stores) {
                List<Metapb.GraphStats> gss = store.getStats()
                                                   .getGraphStatsList();
                for (Metapb.GraphStats gs : gss) {
                    boolean nameEqual = graphName.equals(gs.getGraphName());
                    boolean roleEqual = Metapb.ShardRole.Leader.equals(
                            gs.getRole());
                    if (nameEqual && roleEqual) {
                        dataSize += gs.getApproximateSize();
                    }
                }
            }
            Long size = storages.get(graphSpace);
            size += dataSize;
            storages.put(graphSpace, size);

        }
        Metapb.GraphSpace.Builder spaceBuilder = Metapb.GraphSpace.newBuilder();
        HashMap<String, Boolean> limits = new HashMap<>();
        for (Map.Entry<String, Long> item : storages.entrySet()) {
            String spaceName = item.getKey();
            String value = kvService.get(graphSpaceConfPrefix + spaceName);
            if (!StringUtils.isEmpty(value)) {
                HashMap config = new Gson().fromJson(value, HashMap.class);
                Long size = item.getValue();
                int limit = ((Double) config.get("storage_limit")).intValue();
                long limitByLong = limit * 1024L * 1024L;
                try {
                    spaceBuilder.setName(spaceName).setStorageLimit(limitByLong).setUsedSize(size);
                    Metapb.GraphSpace graphSpace = spaceBuilder.build();
                    configService.setGraphSpace(graphSpace);
                } catch (Exception e) {
                    log.error("update graph space with error:", e);
                }
                // KB and GB * 1024L * 1024L
                if (size > limitByLong) {
                    limits.put(spaceName, true);
                    continue;
                }
            }
            limits.put(spaceName, false);

        }
        GraphState.Builder stateBuilder = GraphState.newBuilder()
                                                    .setMode(GraphMode.ReadOnly)
                                                    .setReason(
                                                            GraphModeReason.Quota);
        for (Metapb.Graph g : graphs) {
            String graphName = g.getGraphName();
            String[] splits = graphName.split(delimiter);
            if (!graphName.endsWith("/g") || splits.length < 2) {
                continue;
            }
            String graphSpace = splits[0];
            Metapb.GraphState gsOld = g.getGraphState();
            GraphMode gmOld = gsOld != null ? gsOld.getMode() : GraphMode.ReadWrite;
            GraphMode gmNew = limits.get(
                    graphSpace) ? GraphMode.ReadOnly : GraphMode.ReadWrite;
            if (gmOld == null || gmOld.getNumber() != gmNew.getNumber()) {
                stateBuilder.setMode(gmNew);
                if (gmNew.getNumber() == GraphMode.ReadOnly.getNumber()) {
                    stateBuilder.setReason(GraphModeReason.Quota);
                }
                GraphState gsNew = stateBuilder.build();
                Metapb.Graph newGraph = g.toBuilder().setGraphState(gsNew)
                                         .build();
                partitionService.updateGraph(newGraph);
                statusListeners.forEach(listener -> {
                    listener.onGraphChange(newGraph, gsOld, gsNew);
                });
            }
        }

        return limits;
    }

    public Runnable getQuotaChecker() {
        return quotaChecker;
    }

    public TaskInfoMeta getTaskInfoMeta() {
        return taskInfoMeta;
    }

    public StoreInfoMeta getStoreInfoMeta() {
        return storeInfoMeta;
    }

    /**
     * 获得分区的Leader
     *
     * @param partition
     * @param initIdx
     * @return
     */
    public Metapb.Shard getLeader(Metapb.Partition partition, int initIdx) {
        Metapb.Shard leader = null;
        try {
            var shardGroup = this.getShardGroup(partition.getId());
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    leader = shard;
                }
            }
        } catch (Exception e) {
            log.error("get leader error: group id:{}, error: {}",
                      partition.getId(), e.getMessage());
        }
        return leader;
    }

    public CacheResponse getCache() throws PDException {

        List<Metapb.Store> stores = getStores();
        List<Metapb.ShardGroup> groups = getShardGroups();
        List<Metapb.Graph> graphs = partitionService.getGraphs();
        CacheResponse cache = CacheResponse.newBuilder().addAllGraphs(graphs)
                                           .addAllShards(groups)
                                           .addAllStores(stores)
                                           .build();
        return cache;
    }
}
