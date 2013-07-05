/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.gregor.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;

import org.I0Itec.zkclient.ZkClient;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.config.ClientConfig;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.store.FileMessageSet;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.store.SegmentInfo;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.test.ClockWatch;


/**
 * Master broker,Mr. Samsa is gregor's farther
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class SamsaMasterBroker extends AbstractBrokerPlugin {

    private static final int MAX_SIZE = 1024 * 1024 * 1024;
    private static final int DEFAULT_CB_THREADPOOL_SIZE = Runtime.getRuntime().availableProcessors() * 3;
    private MetaMorphosisBroker broker;
    private Properties props;
    private SamsaCommandProcessor masterProcessor;
    private RemotingClient remotingClient;
    boolean recoverOffset;

    /**
     * 需要recover的offset信息
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-12-15
     * 
     */
    static class OffsetInfo implements Comparable<OffsetInfo> {
        public final String offsetPath; // 在zk上的路径
        public long msgId; // 消息id
        public long offset; // 绝对偏移量
        public final long oldMsgId;
        private final long oldOffset;


        @Override
        public String toString() {
            return "OffsetInfo [offsetPath=" + this.offsetPath + ", msgId=" + this.msgId + ", offset=" + this.offset
                    + ", oldMsgId=" + this.oldMsgId + ", oldOffset=" + this.oldOffset + "]";
        }


        public OffsetInfo(final String offsetPath, final long msgId, final long offset) {
            super();
            this.offsetPath = offsetPath;
            this.msgId = msgId;
            this.offset = offset;
            this.oldMsgId = msgId;
            this.oldOffset = offset;
        }


        @Override
        public int compareTo(final OffsetInfo o) {
            if (o == null) {
                return 1;
            }
            if (this.msgId > o.msgId) {
                return 1;
            }
            else if (this.msgId < o.msgId) {
                return -1;
            }
            else {
                return 0;
            }
        }

    }

    static class DecodeMessage {
        public final long msgId; // 解出来的消息id
        public final long offset; // 消息的绝对偏移量


        public DecodeMessage(final long msgId, final long offset) {
            super();
            this.msgId = msgId;
            this.offset = offset;
        }

    }

    /**
     * 需要recover的分区
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-12-15
     * 
     */
    static class RecoverPartition implements Comparable<RecoverPartition> {
        private final String topic;
        private final int partition;


        public RecoverPartition(final String topic, final int partition) {
            super();
            this.topic = topic;
            this.partition = partition;
        }


        @Override
        public String toString() {
            return this.topic + "-" + this.partition;
        }


        @Override
        public int compareTo(final RecoverPartition o) {
            final int rt = this.topic.compareTo(o.topic);
            if (rt == 0) {
                return this.partition - o.partition;
            }
            else {
                return rt;
            }
        }

    }


    /**
     * 尽量均匀地根据factor因子划分分区做并行
     * 
     * @param list
     * @param factor
     * @return
     */
    static List<List<RecoverPartition>> fork(final List<RecoverPartition> list, final int factor) {
        final int nPartsPerFork = list.size() / factor;
        final int nForksWithExtraPart = list.size() % factor;
        final List<List<RecoverPartition>> rt = new ArrayList<List<RecoverPartition>>();
        for (int forkPos = 0; forkPos < factor; forkPos++) {
            final int startPart = nPartsPerFork * forkPos + Math.min(forkPos, nForksWithExtraPart);
            final int nParts = nPartsPerFork + (forkPos + 1 > nForksWithExtraPart ? 0 : 1);
            final List<RecoverPartition> forkList = new ArrayList<RecoverPartition>();
            for (int i = startPart; i < startPart + nParts; i++) {
                final RecoverPartition partition = list.get(i);
                forkList.add(partition);
            }
            rt.add(forkList);
        }
        return rt;
    }


    @Override
    public void start() {
        if (!this.recoverOffset) {
            return;
        }

        final MetaZookeeper metaZookeeper = this.broker.getBrokerZooKeeper().getMetaZookeeper();
        final MessageStoreManager storeManager = this.broker.getStoreManager();
        final ZkClient zkClient = metaZookeeper.getZkClient();
        final String consumersPath = metaZookeeper.consumersPath;
        final Set<String> topics = this.broker.getBrokerZooKeeper().getTopics();
        final int brokerId = this.broker.getMetaConfig().getBrokerId();
        final List<String> consumers = ZkUtils.getChildrenMaybeNull(zkClient, consumersPath);
        // 没有订阅者，无需recover
        if (consumers == null || consumers.isEmpty()) {
            this.registerToZk();
            return;
        }

        // 根据cpus和分区数目拆分任务，并行recover,fork/join

        // 所有需要recover的分区
        final List<RecoverPartition> allRecoverParts = new ArrayList<SamsaMasterBroker.RecoverPartition>();
        for (final String topic : topics) {
            final int partitions = this.broker.getStoreManager().getNumPartitions(topic);
            for (int partition = 0; partition < partitions; partition++) {
                allRecoverParts.add(new RecoverPartition(topic, partition));
            }
        }
        // 是否并行recover
        final boolean parallelRecover = Boolean.valueOf(this.props.getProperty("recoverParallel", "true"));
        // 并行线程数
        final int parallelHint =
                Integer.valueOf(this.props.getProperty("recoverParallelHint",
                    String.valueOf(Runtime.getRuntime().availableProcessors())));
        if (parallelRecover) {
            this.recoverParallel(storeManager, zkClient, consumersPath, brokerId, consumers, allRecoverParts,
                parallelHint);
        }
        else {
            final long start = System.currentTimeMillis();
            try {
                this.recoverPartitions(storeManager, zkClient, consumersPath, brokerId, consumers, allRecoverParts);
                log.info("Recover offset successfully in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
            }
            catch (final IOException e) {
                throw new MetamorphosisServerStartupException("Recover offset on startup failed", e);
            }
        }

        // 在recover之后，打开zk注册
        this.registerToZk();
    }


    private void registerToZk() {
        this.broker.getBrokerZooKeeper().getZkConfig().zkEnable = true;
        try {
            this.broker.getBrokerZooKeeper().reRegisterEveryThing();
        }
        catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Register broker to zookeeper failed", e);
        }
    }


    private void recoverParallel(final MessageStoreManager storeManager, final ZkClient zkClient,
            final String consumersPath, final int brokerId, final List<String> consumers,
            final List<RecoverPartition> allRecoverParts, final int parallelHint) {
        log.info("Start to recover offset with " + parallelHint + " threads in parallel");
        final List<List<RecoverPartition>> forks = this.fork(allRecoverParts, parallelHint);
        assert forks.size() == parallelHint;
        final ClockWatch watch = new ClockWatch();
        final CyclicBarrier barrier =
                this.startNRecoverThreads(storeManager, zkClient, consumersPath, brokerId, consumers, parallelHint,
                    forks, watch);
        this.join(watch, barrier);
    }


    private void join(final ClockWatch watch, final CyclicBarrier barrier) {
        try {
            watch.start();
            barrier.await();
            barrier.await();
            log.info("Recover offset successfully in " + watch.getDurationInMillis() / 1000 + " seconds");
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (final Exception e) {
            SamsaMasterBroker.log.error("Broken barrier", e);
        }
    }


    private CyclicBarrier startNRecoverThreads(final MessageStoreManager storeManager, final ZkClient zkClient,
            final String consumersPath, final int brokerId, final List<String> consumers, final int parallelHint,
            final List<List<RecoverPartition>> forks, final ClockWatch watch) {
        // 启动parallelHint个线程
        final CyclicBarrier barrier = new CyclicBarrier(parallelHint + 1, watch);
        for (int i = 0; i < parallelHint; i++) {
            final List<RecoverPartition> recoverParts = forks.get(i);
            new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        SamsaMasterBroker.this.recoverPartitions(storeManager, zkClient, consumersPath, brokerId,
                            consumers, recoverParts);
                        barrier.await();
                    }
                    catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (final Exception e) {
                        SamsaMasterBroker.log.error("Broken barrier", e);
                    }
                }
            }.start();
        }
        return barrier;
    }


    private void recoverPartitions(final MessageStoreManager storeManager, final ZkClient zkClient,
            final String consumersPath, final int brokerId, final List<String> consumers,
            final List<RecoverPartition> recoverParts) throws IOException {
        // 遍历topic,partition,consumer
        for (final RecoverPartition recoverPartition : recoverParts) {
            try {
                final MessageStore store =
                        storeManager.getOrCreateMessageStore(recoverPartition.topic, recoverPartition.partition);
                if (store == null) {
                    log.warn("Could not find partition:" + recoverPartition);
                    continue;
                }
                final List<SegmentInfo> segmentInfos = store.getSegmentInfos();
                if (segmentInfos.isEmpty()) {
                    log.warn("Partition:" + recoverPartition + " is empty");
                    continue;
                }
                // offset信息集合，按照msgId大小排序
                final TreeMap<Long, List<OffsetInfo>> offsetInfos =
                        this.getOffsetInfosFromZk(zkClient, consumersPath, brokerId, consumers, recoverPartition);

                // 遍历分区做recover
                // 遍历分区内的文件，从后往前遍历
                Collections.reverse(segmentInfos);
                // recover成功的offset列表
                final List<OffsetInfo> recoveredOffsetInfos = new ArrayList<SamsaMasterBroker.OffsetInfo>();
                this.recoverSegments(recoverPartition, store, segmentInfos, offsetInfos, recoveredOffsetInfos);
                // 更新到zookeeper
                this.update2zk(zkClient, offsetInfos, recoveredOffsetInfos);
            }
            catch (final IOException e) {
                log.error("Unexpected IOException occured when recovering partition=" + recoverPartition);
                throw e;
            }
        }
    }


    private TreeMap<Long, List<OffsetInfo>> getOffsetInfosFromZk(final ZkClient zkClient, final String consumersPath,
        final int brokerId, final List<String> consumers, final RecoverPartition recoverPartition) {
        final TreeMap<Long, List<OffsetInfo>> offsetInfos = new TreeMap<Long, List<OffsetInfo>>();

        // 从zk上获取需要recover的offset信息
        for (final String consumer : consumers) {
            final String offsetPath =
                    consumersPath + "/" + consumer + "/offsets/" + recoverPartition.topic + "/" + brokerId + "-"
                            + recoverPartition.partition;
            // 存在offsetPath，则进行修正
            if (ZkUtils.pathExists(zkClient, offsetPath)) {
                final String value = ZkUtils.readData(zkClient, offsetPath);
                if (StringUtils.isBlank(value)) {
                    continue;
                }
                final OffsetInfo info = this.readOffsetInfo(offsetPath, value);
                // 只需要recover有效的info
                if (info != null && info.msgId > 0) {
                    List<OffsetInfo> list = offsetInfos.get(info.msgId);
                    if (list == null) {
                        list = new ArrayList<SamsaMasterBroker.OffsetInfo>();
                        offsetInfos.put(info.msgId, list);
                    }
                    list.add(info);
                }
            }
        }
        return offsetInfos;
    }


    private void recoverSegments(final RecoverPartition recoverPartition, final MessageStore store,
            final List<SegmentInfo> segmentInfos, final TreeMap<Long, List<OffsetInfo>> offsetInfos,
            final List<OffsetInfo> recoveredOffsetInfos) throws IOException {
        for (final SegmentInfo segInfo : segmentInfos) {
            // 没有需要纠偏的offset了，中断
            if (offsetInfos.isEmpty()) {
                break;
            }
            this.recoverSegment(recoverPartition.topic, store, offsetInfos, recoveredOffsetInfos, segInfo);
        }
    }


    private void update2zk(final ZkClient zkClient, final TreeMap<Long, List<OffsetInfo>> offsetInfos,
            final List<OffsetInfo> recoveredOffsetInfos) {
        if (!recoveredOffsetInfos.isEmpty()) {
            for (final OffsetInfo recoverOffsetInfo : recoveredOffsetInfos) {
                // 有变更的才需要更新，减少对zk压力
                if (recoverOffsetInfo.oldOffset != recoverOffsetInfo.offset
                        || recoverOffsetInfo.oldMsgId != recoverOffsetInfo.msgId) {
                    final String newInfo = recoverOffsetInfo.msgId + "-" + recoverOffsetInfo.offset;
                    try {
                        ZkUtils.updatePersistentPath(zkClient, recoverOffsetInfo.offsetPath, newInfo);
                    }
                    catch (final Exception e) {
                        log.error(
                            "Recover offset for " + recoverOffsetInfo.offsetPath + " failed, new info:" + newInfo, e);
                    }
                }
            }

            // 没有recover的offset信息，只做日志，理论上不应该出现这种情况
            for (final List<OffsetInfo> list : offsetInfos.values()) {
                for (final OffsetInfo recoverOffsetInfo : list) {
                    log.warn("We don't recover " + recoverOffsetInfo.offsetPath + ":msgId="
                            + recoverOffsetInfo.oldMsgId + ",offset=" + recoverOffsetInfo.oldOffset);
                }
            }
        }
        else {
            // 这种情况下应该是slave分区没有消息，全部都要纠偏到0
            for (final List<OffsetInfo> list : offsetInfos.values()) {
                // msgId不为-1的才纠偏，减少对zk压力
                for (final OffsetInfo recoverOffsetInfo : list) {
                    if (recoverOffsetInfo.oldMsgId != -1L) {
                        final String newInfo = "-1-0";
                        try {
                            ZkUtils.updatePersistentPath(zkClient, recoverOffsetInfo.offsetPath, newInfo);
                        }
                        catch (final Exception e) {
                            log.error("Recover offset for " + recoverOffsetInfo.offsetPath + " failed, new info:"
                                    + newInfo, e);
                        }
                    }
                }
            }
        }
    }


    private void recoverSegment(final String topic, final MessageStore store,
            final TreeMap<Long, List<OffsetInfo>> offsetInfos, final List<OffsetInfo> recoveredOffsetInfos,
            final SegmentInfo segInfo) throws IOException {
        final long minOffset = segInfo.startOffset;
        final long size = segInfo.size;
        final long maxOffset = minOffset + size;
        long startOffset = minOffset;
        FileMessageSet msgSet = null;
        // 本segment纠偏的offset集合
        final Set<OffsetInfo> segRecoverOffsetInfos = new HashSet<SamsaMasterBroker.OffsetInfo>();
        // 从前向后读文件
        while (startOffset < maxOffset && (msgSet = (FileMessageSet) store.slice(startOffset, MAX_SIZE)) != null) {
            final int sizeInBytes = (int) msgSet.getSizeInBytes();
            final ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes);
            msgSet.read(buffer);
            final MessageIterator it = new MessageIterator(topic, buffer.array());
            final List<DecodeMessage> msgList = new ArrayList<DecodeMessage>();
            // 遍历消息
            long msgOffset = 0;
            while (it.hasNext()) {
                try {
                    final Message msg = it.next();
                    msgList.add(new DecodeMessage(msg.getId(), startOffset + it.getOffset()));
                    msgOffset = it.getOffset();
                }
                catch (final InvalidMessageException e) {
                    // 理论上不会遇到这种情况，预防万一还是打印日志
                    log.error("Message was corrupted,partition=" + store.getDescription() + ",offset=" + msgOffset);
                }
            }
            // recover这一批消息
            this.recoverOffset(offsetInfos, segRecoverOffsetInfos, msgList);
            // 往前移动startOffset
            startOffset = startOffset + it.getOffset();
        }

        // 移除本segment能够纠偏的offset
        for (final OffsetInfo info : segRecoverOffsetInfos) {
            offsetInfos.remove(info.oldMsgId);
        }
        // final Iterator<Entry<Long, List<OffsetInfo>>> it =
        // offsetInfos.entrySet().iterator();
        // while (it.hasNext()) {
        // final Entry<Long, List<OffsetInfo>> entry = it.next();
        // final Long msgId = entry.getKey();
        // final List<OffsetInfo> value = entry.getValue();
        // if (!segRecoverOffsetInfos.contains(value.get(0))) {
        // it.remove();
        // }
        // }
        // 添加到公共集合，做集中更新到zk
        recoveredOffsetInfos.addAll(segRecoverOffsetInfos);
    }


    private void recoverOffset(final TreeMap<Long, List<OffsetInfo>> offsetInfos,
            final Set<OffsetInfo> segRecoverOffsetInfos, final List<DecodeMessage> msgList) {

        for (final DecodeMessage decodeMsg : msgList) {
            // 返回大于或者等于当前messageId的子集合，这个集合需要纠偏,这个过程会在本segment持续多次
            final SortedMap<Long, List<OffsetInfo>> subMap = offsetInfos.tailMap(decodeMsg.msgId);
            // 这个子集合的offset就纠偏到这里
            if (!subMap.isEmpty()) {
                for (final List<OffsetInfo> offsetInfoList : subMap.values()) {
                    for (final OffsetInfo offsetInfo : offsetInfoList) {
                        if (offsetInfo.offset != decodeMsg.offset) {
                            // 纠偏offset和msgId
                            offsetInfo.offset = decodeMsg.offset;
                            offsetInfo.msgId = decodeMsg.msgId;
                            // 加入修改集合
                            segRecoverOffsetInfos.add(offsetInfo);
                        }
                    }
                }
            }
        }

    }


    static OffsetInfo readOffsetInfo(final String path, final String offsetString) {
        final int index = offsetString.lastIndexOf("-");
        if (index > 0) {
            // 仅支持1.4开始的新客户端
            final long msgId = Long.parseLong(offsetString.substring(0, index));
            final long offset = Long.parseLong(offsetString.substring(index + 1));
            return new OffsetInfo(path, msgId, offset);
        }
        else {
            log.warn("Skipped old consumers which version is before 1.4. The path:" + path + " and The value:"
                    + offsetString);
            return null;
        }
    }


    @Override
    public void stop() {
        if (this.remotingClient != null) {
            try {
                this.remotingClient.stop();
            }
            catch (final NotifyRemotingException e) {
                log.error("Stop remoting client failed", e);
            }
        }
    }

    private long sendToSlaveTimeoutInMills = 2000;

    private long checkSlaveIntervalInMills = 100;

    private int slaveContinuousFailureThreshold = 100;


    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        this.broker = metaMorphosisBroker;
        this.props = props;
        if (this.props == null) {
            throw new MetamorphosisServerStartupException("Null samsa_master properties");
        }
        this.recoverOffset = Boolean.valueOf(this.props.getProperty("recoverOffset", "false"));
        // 需要recover offset，暂时先不发布到zookeeper上，在recover之后会注册上去
        if (this.recoverOffset) {
            this.broker.getBrokerZooKeeper().getZkConfig().zkEnable = false;
        }
        final int callbackThreadCount =
                Integer.parseInt(props.getProperty("callbackThreadCount", String.valueOf(DEFAULT_CB_THREADPOOL_SIZE)));
        final String slave = props.getProperty("slave");
        if (StringUtils.isBlank(slave)) {
            throw new IllegalArgumentException("Blank slave");
        }
        this.setConfigs(props);
        final ClientConfig clientConfig = new ClientConfig();
        // 只使用1个reactor
        clientConfig.setSelectorPoolSize(1);
        clientConfig.setWireFormatType(new MetamorphosisWireFormatType());
        try {
            this.remotingClient = RemotingFactory.newRemotingClient(clientConfig);
            this.remotingClient.start();
            this.masterProcessor =
                    new SamsaCommandProcessor(metaMorphosisBroker.getStoreManager(),
                        metaMorphosisBroker.getExecutorsManager(), metaMorphosisBroker.getStatsManager(),
                        metaMorphosisBroker.getRemotingServer(), metaMorphosisBroker.getMetaConfig(),
                        metaMorphosisBroker.getIdWorker(), metaMorphosisBroker.getBrokerZooKeeper(),
                        this.remotingClient, metaMorphosisBroker.getConsumerFilterManager(), slave,
                        callbackThreadCount, this.sendToSlaveTimeoutInMills, this.checkSlaveIntervalInMills,
                        this.slaveContinuousFailureThreshold);
            // 替换处理器
            this.broker.setBrokerProcessor(this.masterProcessor);
            log.info("Init samsa mater successfully with config:" + props);
        }
        catch (final NotifyRemotingException e) {
            throw new MetamorphosisServerStartupException("Init master processor failed", e);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private void setConfigs(final Properties props) {
        if (!StringUtils.isBlank(props.getProperty("sendToSlaveTimeoutInMills"))) {
            this.sendToSlaveTimeoutInMills = Long.parseLong(props.getProperty("sendToSlaveTimeoutInMills"));
            if (this.sendToSlaveTimeoutInMills <= 0) {
                throw new IllegalArgumentException("Invalid sendToSlaveTimeoutInMills value");
            }
        }
        if (!StringUtils.isBlank(props.getProperty("checkSlaveIntervalInMills"))) {
            this.checkSlaveIntervalInMills = Long.parseLong(props.getProperty("checkSlaveIntervalInMills"));
            if (this.checkSlaveIntervalInMills <= 0) {
                throw new IllegalArgumentException("Invalid checkSlaveIntervalInMills value");
            }
        }
        if (!StringUtils.isBlank(props.getProperty("slaveContinuousFailureThreshold"))) {
            this.slaveContinuousFailureThreshold =
                    Integer.parseInt(props.getProperty("slaveContinuousFailureThreshold"));
            if (this.slaveContinuousFailureThreshold <= 0) {
                throw new IllegalArgumentException("Invalid slaveContinuousFailureThreshold value");
            }
        }
    }


    @Override
    public String name() {
        return "samsa";
    }

}