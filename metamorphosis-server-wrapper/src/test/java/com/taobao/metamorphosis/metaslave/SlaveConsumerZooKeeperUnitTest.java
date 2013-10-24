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
package com.taobao.metamorphosis.metaslave;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerZooKeeperAccessor;
import com.taobao.metamorphosis.client.consumer.DefaultLoadBalanceStrategy;
import com.taobao.metamorphosis.client.consumer.FetchManager;
import com.taobao.metamorphosis.client.consumer.FetchRequest;
import com.taobao.metamorphosis.client.consumer.LoadBalanceStrategy;
import com.taobao.metamorphosis.client.consumer.SubscriberInfo;
import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.cluster.json.TopicBroker;
import com.taobao.metamorphosis.metaslave.SlaveConsumerZooKeeper.SlaveZKLoadRebalanceListener;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * 
 * @author 无花
 * @since 2011-6-29 下午05:00:22
 */

public class SlaveConsumerZooKeeperUnitTest {

    private static final String GROUP = "meta-slave-group";
    private ZKConfig zkConfig;
    private ZkClient client;
    // private DiamondManager diamondManager;

    private RemotingClientWrapper remotingClient;
    private FetchManager fetchManager;
    private OffsetStorage offsetStorage;
    private IMocksControl mocksControl;
    private SlaveConsumerZooKeeper slaveConsumerZooKeeper;
    private final int brokerId = 0;
    private LoadBalanceStrategy loadBalanceStrategy;
    private MetaZookeeper metaZookeeper;


    @Before
    public void setUp() {
        this.mocksControl = EasyMock.createControl();
        this.offsetStorage = this.mocksControl.createMock(OffsetStorage.class);
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.fetchManager = this.mocksControl.createMock(FetchManager.class);

        // this.diamondManager = new DefaultDiamondManager(null,
        // "metamorphosis.testZkConfig", (ManagerListener) null);
        this.zkConfig = new ZKConfig();// DiamondUtils.getZkConfig(this.diamondManager,
        // 10000);
        this.zkConfig.zkConnect = "localhost:2181";
        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
        this.metaZookeeper = new MetaZookeeper(this.client, this.zkConfig.zkRoot);
        this.loadBalanceStrategy = new DefaultLoadBalanceStrategy();
        this.slaveConsumerZooKeeper =
                new SlaveConsumerZooKeeper(this.metaZookeeper, this.remotingClient, this.client, this.zkConfig,
                    this.brokerId);
    }


    @Test
    public void testReigsterSlaveConsumer() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroup(GROUP);
        final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry =
                new ConcurrentHashMap<String, SubscriberInfo>();
        topicSubcriberRegistry.put("topic1", new SubscriberInfo(null, null, 1024 * 1024));
        topicSubcriberRegistry.put("topic2", new SubscriberInfo(null, null, 1024 * 1024));

        // 假设集群里有两台master,topic1在master里有3个分区;
        // topic2在master里有1个分区,在另一个不相关的master里有1个分区
        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master", "meta://localhost:0");
        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/1/master", "meta://localhost:1");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsSubPath + "/topic1/0-m",
            new TopicBroker(3, "0-m").toJson());
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsSubPath + "/topic2/0-m",
            new TopicBroker(1, "0-m").toJson());
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsSubPath + "/topic2/1-m",
            new TopicBroker(1, "1-m").toJson());

        this.mockConnect("meta://localhost:0");
        // this.mockConnect("meta://localhost:1");不连接到另外一个master
        this.mockCommitOffsets(GROUP, new ArrayList<TopicPartitionRegInfo>());
        this.mockLoadNullInitOffset("topic1", GROUP, new Partition("0-0"));
        this.mockLoadNullInitOffset("topic1", GROUP, new Partition("0-1"));
        this.mockLoadNullInitOffset("topic1", GROUP, new Partition("0-2"));
        this.mockLoadNullInitOffset("topic2", GROUP, new Partition("0-0"));
        // this.mockLoadNullInitOffset("topic2", GROUP, new
        // Partition("1-0"));不load另外一个master的分区
        this.mockFetchManagerRestart();
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), 0), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), 0), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-2"), 0), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("0-0"), 0), 1024 * 1024));
        // this.mockAddFetchRequest(new FetchRequest(new Broker(1,
        // "meta://localhost:1"), 0, new TopicPartitionRegInfo(
        // "topic2", new Partition("1-0"), 0), 1024 * 1024));不向另外一个master抓取消息

        this.mocksControl.replay();

        this.slaveConsumerZooKeeper.registerConsumer(consumerConfig, this.fetchManager, topicSubcriberRegistry,
            this.offsetStorage, this.loadBalanceStrategy);
        this.mocksControl.verify();

        // 验证订阅者分配,只订阅自己master下的所有分区下
        final SlaveZKLoadRebalanceListener listener = this.checkTopic();

        final Set<Broker> brokerSet = ConsumerZooKeeperAccessor.getOldBrokerSet(listener);
        assertEquals(1, brokerSet.size());
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertFalse(brokerSet.contains(new Broker(1, "meta://localhost:1")));
    }


    private SlaveZKLoadRebalanceListener checkTopic() {
        final SlaveZKLoadRebalanceListener listener =
                (SlaveZKLoadRebalanceListener) ConsumerZooKeeperAccessor.getBrokerConnectionListenerForTest(
                    this.slaveConsumerZooKeeper, this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                ConsumerZooKeeperAccessor.getTopicRegistry(listener);
        assertNotNull(topicRegistry);
        assertFalse(topicRegistry.isEmpty());
        assertEquals(2, topicRegistry.size());

        assertTrue(topicRegistry.containsKey("topic1"));
        assertTrue(topicRegistry.containsKey("topic2"));
        this.checkTopic1(topicRegistry);
        this.checkTopic2(topicRegistry);
        return listener;
    }


    @Test
    public void testReigsterSlaveConsumer_thenMasterDown() throws Exception {
        this.testReigsterSlaveConsumer();
        this.mocksControl.reset();
        this.mockCommitOffsets(GROUP,
            ConsumerZooKeeperAccessor.getTopicPartitionRegInfos(this.slaveConsumerZooKeeper, this.fetchManager));

        this.mockFetchManagerRestartAnyTimes();
        this.mockConnectCloseAnyTimes("meta://localhost:0");
        this.mocksControl.replay();
        // master down
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master");
        // 这里topic的两次删除(挂掉或人工停掉),可能会引起两次负载均衡
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsSubPath + "/topic1/0-m");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsSubPath + "/topic2/0-m");
        Thread.sleep(5000);
        this.mocksControl.verify();

        // master 挂掉或人工停掉,TopicPartitionRegInfo清空了
        final SlaveZKLoadRebalanceListener listener =
                (SlaveZKLoadRebalanceListener) ConsumerZooKeeperAccessor.getBrokerConnectionListenerForTest(
                    this.slaveConsumerZooKeeper, this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                ConsumerZooKeeperAccessor.getTopicRegistry(listener);
        assertNotNull(topicRegistry);
        assertTrue(topicRegistry.isEmpty());
        final Set<Broker> brokerSet = ConsumerZooKeeperAccessor.getOldBrokerSet(listener);
        assertEquals(0, brokerSet.size());
        assertFalse(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertFalse(brokerSet.contains(new Broker(1, "meta://localhost:1")));
    }


    @Test
    public void testReigsterSlaveConsumer_thenMasterDown_thenMasterStart() throws Exception {
        this.testReigsterSlaveConsumer_thenMasterDown();
        this.mocksControl.reset();
        this.mockConnect("meta://localhost:0");
        this.mockCommitOffsets(GROUP, new ArrayList<TopicPartitionRegInfo>());
        this.mockLoadNullInitOffset("topic1", GROUP, new Partition("0-0"));
        this.mockLoadNullInitOffset("topic1", GROUP, new Partition("0-1"));
        this.mockLoadNullInitOffset("topic1", GROUP, new Partition("0-2"));
        this.mockLoadNullInitOffset("topic2", GROUP, new Partition("0-0"));
        this.mockFetchManagerRestart();
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), 0), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), 0), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-2"), 0), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("0-0"), 0), 1024 * 1024));
        this.mocksControl.replay();

        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master", "meta://localhost:0");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsSubPath + "/topic1/0-m",
            new TopicBroker(3, "0-m").toJson());
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsSubPath + "/topic2/0-m",
            new TopicBroker(1, "0-m").toJson());
        Thread.sleep(5000);
        this.mocksControl.verify();

        // 恢复到testReigsterSlaveConsumer时的状态
        final SlaveZKLoadRebalanceListener listener = this.checkTopic();
        final Set<Broker> brokerSet = ConsumerZooKeeperAccessor.getOldBrokerSet(listener);
        assertEquals(1, brokerSet.size());
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertFalse(brokerSet.contains(new Broker(1, "meta://localhost:1")));

    }


    @Test
    public void testReigsterSlaveConsumer_thenOtherMasterDown() throws Exception {
        this.testReigsterSlaveConsumer();
        this.mocksControl.reset();
        // mock nothing
        this.mocksControl.replay();
        // other master down,no problem
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/1/master");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsSubPath + "/topic2/1-m");
        Thread.sleep(5000);
        this.mocksControl.verify();
        final SlaveZKLoadRebalanceListener listener = this.checkTopic();

        final Set<Broker> brokerSet = ConsumerZooKeeperAccessor.getOldBrokerSet(listener);
        assertEquals(1, brokerSet.size());
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertFalse(brokerSet.contains(new Broker(1, "meta://localhost:1")));
    }


    private void checkTopic1(
            final ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry) {
        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap1 = topicRegistry.get("topic1");
        assertNotNull(partMap1);
        assertEquals(3, partMap1.size());
        assertTrue(partMap1.containsKey(new Partition("0-0")));
        assertTrue(partMap1.containsKey(new Partition("0-1")));
        assertTrue(partMap1.containsKey(new Partition("0-2")));
    }


    private void checkTopic2(
            final ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry) {
        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap2 = topicRegistry.get("topic2");
        assertNotNull(partMap2);
        assertEquals(1, partMap2.size());
        assertTrue(partMap2.containsKey(new Partition("0-0")));
        assertFalse(partMap2.containsKey(new Partition("1-0")));
    }


    private void mockConnect(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.connectWithRef(EasyMock.eq(url), EasyMock.anyObject());
        EasyMock.expectLastCall();
        this.remotingClient.awaitReadyInterrupt(url, 4000);
        EasyMock.expectLastCall();
    }


    private void mockCommitOffsets(final String group, final Collection<TopicPartitionRegInfo> arrayList) {
        this.offsetStorage.commitOffset(group, arrayList);
        EasyMock.expectLastCall();
    }


    private void mockLoadNullInitOffset(final String topic, final String group, final Partition partition) {
        EasyMock.expect(this.offsetStorage.load(EasyMock.eq(topic), EasyMock.contains(group), EasyMock.eq(partition)))
        .andReturn(null);
        this.offsetStorage.initOffset(EasyMock.eq(topic), EasyMock.contains(group), EasyMock.eq(partition),
            EasyMock.eq(0L));
        EasyMock.expectLastCall();
    }


    private void mockFetchManagerRestart() throws InterruptedException {
        this.fetchManager.stopFetchRunner();
        EasyMock.expectLastCall();
        this.fetchManager.resetFetchState();
        EasyMock.expectLastCall();
        this.fetchManager.startFetchRunner();
        EasyMock.expectLastCall();
    }


    private void mockFetchManagerRestartAnyTimes() throws InterruptedException {
        this.fetchManager.stopFetchRunner();
        EasyMock.expectLastCall().anyTimes();
        this.fetchManager.resetFetchState();
        EasyMock.expectLastCall().anyTimes();
        this.fetchManager.startFetchRunner();
        EasyMock.expectLastCall().anyTimes();
    }


    private void mockAddFetchRequest(final FetchRequest fetchRequest) {
        this.fetchManager.addFetchRequest(fetchRequest);
        EasyMock.expectLastCall();
    }


    private void mockConnectCloseAnyTimes(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.closeWithRef(EasyMock.eq(url), EasyMock.anyObject(), EasyMock.eq(false));
        EasyMock.expectLastCall().anyTimes();
    }


    @After
    public void tearDown() throws Exception {
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/1/master");
        // this.diamondManager.close();
        this.client.close();
    }
}