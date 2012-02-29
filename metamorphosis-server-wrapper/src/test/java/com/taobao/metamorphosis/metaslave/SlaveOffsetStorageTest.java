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

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-7-1 ÏÂÎç01:59:16
 */

public class SlaveOffsetStorageTest {
    private static final String GROUP = "meta-slave-group";
    private MetaMorphosisBroker broker;
    private RemotingClientWrapper remotingClient;
    private SlaveZooKeeper slaveZooKeeper;
    private MessageStoreManager storeManager;
    private MessageStore store;
    private SlaveOffsetStorage slaveOffsetStorage;
    private IMocksControl mocksControl;
    private BrokerCommandProcessor brokerCommandProcessor;


    @Before
    public void setup() {
        this.mocksControl = EasyMock.createControl();
        this.broker = this.mocksControl.createMock(MetaMorphosisBroker.class);
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.slaveZooKeeper = this.mocksControl.createMock(SlaveZooKeeper.class);
        this.storeManager = this.mocksControl.createMock(MessageStoreManager.class);
        this.store = this.mocksControl.createMock(MessageStore.class);
        this.slaveOffsetStorage = new SlaveOffsetStorage(this.broker, this.slaveZooKeeper, this.remotingClient);
        this.brokerCommandProcessor = this.mocksControl.createMock(BrokerCommandProcessor.class);
    }


    @Test
    public void testLoad_hasLocalMessageFile() {
        final String topic = "topictest";
        final int partition = 1;
        // EasyMock.expect(this.broker.getCommandProcessor()).andReturn(this.brokerCommandProcessor);
        EasyMock.expect(this.broker.getStoreManager()).andReturn(this.storeManager);
        EasyMock.expect(this.storeManager.getMessageStore(topic, partition)).andReturn(this.store);
        EasyMock.expect(this.store.getSizeInBytes()).andReturn(555L).anyTimes();
        EasyMock.expect(this.store.getMinOffset()).andReturn(100L).once();
        this.mocksControl.replay();
        final TopicPartitionRegInfo info = this.slaveOffsetStorage.load(topic, "ss", new Partition(0, partition));
        this.mocksControl.verify();

        Assert.assertEquals(100L + 555L, info.getOffset().get());
        Assert.assertEquals(partition, info.getPartition().getPartition());
        Assert.assertEquals(topic, info.getTopic());
    }


    @Test
    public void testLoad_fromMaster() throws Exception {
        final String topic = "topictest";
        final int partition = 1;
        final String masterUrl = "meta://testhost:8123";
        final BooleanCommand resp = new BooleanCommand(0, HttpStatus.Success, "555");
        // EasyMock.expect(this.broker.getCommandProcessor()).andReturn(this.brokerCommandProcessor).anyTimes();
        EasyMock.expect(this.broker.getStoreManager()).andReturn(this.storeManager);
        EasyMock.expect(this.storeManager.getMessageStore(topic, partition)).andReturn(null);
        EasyMock.expect(this.slaveZooKeeper.getMasterServerUrl()).andReturn(masterUrl);
        EasyMock.expect(this.remotingClient.isConnected(masterUrl)).andReturn(true);
        OpaqueGenerator.resetOpaque();
        EasyMock.expect(
            this.remotingClient.invokeToGroup(masterUrl,
                new OffsetCommand(topic, GROUP, partition, 0, OpaqueGenerator.getNextOpaque()))).andReturn(resp);
        OpaqueGenerator.resetOpaque();
        final MetaConfig config = new MetaConfig();
        config.setSlaveGroup(GROUP);
        EasyMock.expect(this.broker.getMetaConfig()).andReturn(config);
        this.mocksControl.replay();
        final TopicPartitionRegInfo info = this.slaveOffsetStorage.load(topic, "ss", new Partition(0, partition));
        this.mocksControl.verify();

        Assert.assertEquals(555L, info.getOffset().get());
        Assert.assertEquals(partition, info.getPartition().getPartition());
        Assert.assertEquals(topic, info.getTopic());
    }


    @Test(expected = RuntimeException.class)
    public void testLoad_throwException() throws Exception {
        final String topic = "topictest";
        final int partition = 1;
        // EasyMock.expect(this.broker.getCommandProcessor()).andReturn(this.brokerCommandProcessor).anyTimes();
        EasyMock.expect(this.broker.getStoreManager()).andReturn(this.storeManager);
        EasyMock.expect(this.storeManager.getMessageStore(topic, partition)).andReturn(null);
        EasyMock.expect(this.slaveZooKeeper.getMasterServerUrl()).andReturn(null);
        this.mocksControl.replay();
        this.slaveOffsetStorage.load(topic, "ss", new Partition(0, partition));
        this.mocksControl.verify();
    }


    @Test
    public void testLoad_failQuery() throws Exception {
        final String topic = "topictest";
        final int partition = 1;
        final String masterUrl = "meta://testhost:8123";
        final BooleanCommand resp = new BooleanCommand(0, HttpStatus.NotFound, "");
        // EasyMock.expect(this.broker.getCommandProcessor()).andReturn(this.brokerCommandProcessor).anyTimes();
        EasyMock.expect(this.broker.getStoreManager()).andReturn(this.storeManager);
        EasyMock.expect(this.storeManager.getMessageStore(topic, partition)).andReturn(null);
        EasyMock.expect(this.slaveZooKeeper.getMasterServerUrl()).andReturn(masterUrl);
        EasyMock.expect(this.remotingClient.isConnected(masterUrl)).andReturn(true);
        OpaqueGenerator.resetOpaque();
        EasyMock.expect(
            this.remotingClient.invokeToGroup(masterUrl,
                new OffsetCommand(topic, GROUP, partition, 0, OpaqueGenerator.getNextOpaque()))).andReturn(resp);
        OpaqueGenerator.resetOpaque();
        final MetaConfig config = new MetaConfig();
        config.setSlaveGroup(GROUP);
        EasyMock.expect(this.broker.getMetaConfig()).andReturn(config);
        this.mocksControl.replay();
        final TopicPartitionRegInfo info = this.slaveOffsetStorage.load(topic, "ss", new Partition(0, partition));
        this.mocksControl.verify();
        Assert.assertEquals(0L, info.getOffset().get());
        Assert.assertEquals(partition, info.getPartition().getPartition());
        Assert.assertEquals(topic, info.getTopic());
    }


    @Test(expected = RuntimeException.class)
    public void testLoad_failQuery2() throws Exception {
        final String topic = "topictest";
        final int partition = 1;
        final String masterUrl = "meta://testhost:8123";
        // EasyMock.expect(this.broker.getCommandProcessor()).andReturn(this.brokerCommandProcessor).anyTimes();
        EasyMock.expect(this.broker.getStoreManager()).andReturn(this.storeManager);
        EasyMock.expect(this.storeManager.getMessageStore(topic, partition)).andReturn(null);
        EasyMock.expect(this.slaveZooKeeper.getMasterServerUrl()).andReturn(masterUrl);
        EasyMock.expect(this.remotingClient.isConnected(masterUrl)).andReturn(true);
        EasyMock.expect(this.remotingClient.invokeToGroup(masterUrl, new OffsetCommand(topic, GROUP, partition, 0, 0)))
            .andThrow(new Exception("unknown error when invoke"));
        this.mocksControl.replay();
        this.slaveOffsetStorage.load(topic, "ss", new Partition(0, partition));
        this.mocksControl.verify();
    }
}