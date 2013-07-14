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
package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.DataCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.OffsetCommand;


public class SimpleMessageConsumerUnitTest {

    protected SimpleMessageConsumer consumer;
    protected ConsumerZooKeeper consumerZooKeeper;
    protected ProducerZooKeeper producerZooKeeper;
    protected OffsetStorage offsetStorage;
    protected IMocksControl mocksControl;
    protected ConsumerConfig consumerConfig;
    protected SubscribeInfoManager subscribeInfoManager;
    protected RecoverStorageManager recoverStorageManager;
    protected RemotingClientWrapper remotingClient;
    protected LoadBalanceStrategy loadBalanceStrategy;


    @Before
    public void setUp() {
        this.mocksControl = EasyMock.createControl();
        this.consumerZooKeeper = this.mocksControl.createMock(ConsumerZooKeeper.class);
        this.producerZooKeeper = this.mocksControl.createMock(ProducerZooKeeper.class);
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.offsetStorage = this.mocksControl.createMock(OffsetStorage.class);
        this.subscribeInfoManager = new SubscribeInfoManager();
        this.recoverStorageManager = this.mocksControl.createMock(RecoverStorageManager.class);
        this.consumerConfig = new ConsumerConfig();
        this.loadBalanceStrategy = new DefaultLoadBalanceStrategy();
        this.consumerConfig.setGroup("boyan");
        this.consumer =
                new SimpleMessageConsumer(null, this.remotingClient, this.consumerConfig, this.consumerZooKeeper,
                    this.producerZooKeeper, this.subscribeInfoManager, this.recoverStorageManager, this.offsetStorage,
                    this.loadBalanceStrategy);
        EasyMock.makeThreadSafe(this.consumerZooKeeper, true);
        EasyMock.makeThreadSafe(this.producerZooKeeper, true);
        EasyMock.makeThreadSafe(this.remotingClient, true);
        EasyMock.makeThreadSafe(this.offsetStorage, true);
        EasyMock.makeThreadSafe(this.recoverStorageManager, true);
    }


    @Test
    public void testSubscribeCompleteSubscribe() throws Exception {
        final String topic = "topic1";
        final int maxSize = 1024;
        final MessageListener messageListener = new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {

            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        };
        final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry =
                new ConcurrentHashMap<String, SubscriberInfo>();
        topicSubcriberRegistry.put(topic, new SubscriberInfo(messageListener, null, maxSize));
        this.consumerZooKeeper.registerConsumer(this.consumerConfig, this.consumer.getFetchManager(),
            topicSubcriberRegistry, this.offsetStorage, this.loadBalanceStrategy);
        EasyMock.expectLastCall();
        this.mocksControl.replay();
        this.consumer.subscribe(topic, maxSize, messageListener).completeSubscribe();
        this.mocksControl.verify();
        assertEquals(topicSubcriberRegistry, this.consumer.getTopicSubcriberRegistry());
    }


    @Test(expected = MetaClientException.class)
    public void testSubscribeDuplicate() throws Exception {
        final String topic = "topic1";
        final int maxSize = 1024;
        final MessageListener messageListener = new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {

            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        };
        this.consumer.subscribe(topic, maxSize, messageListener).subscribe(topic, maxSize, messageListener);

    }


    @Test
    public void testGet() throws Exception {
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final byte[] data = "hello".getBytes();
        final String url = "meta://localhost:0";
        final MessageIterator messageIterator = new MessageIterator(topic, data);

        this.producerZooKeeper.publishTopic(topic, this.consumer);
        EasyMock.expect(this.remotingClient.isConnected(url)).andReturn(true);
        EasyMock.expectLastCall();
        EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(url);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(url,
                new GetCommand(topic, this.consumerConfig.getGroup(), partition.getPartition(), offset, maxSize,
                    Integer.MIN_VALUE), 10000, TimeUnit.MILLISECONDS)).andReturn(
                        new DataCommand(data, Integer.MIN_VALUE));
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        assertEquals(messageIterator, this.consumer.get(topic, partition, offset, maxSize));
        this.mocksControl.verify();
    }


    @Test
    public void testOffset() throws Exception {
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final String url = "meta://localhost:8123";

        final Broker broker = new Broker(1, url);
        final long delay = 0;
        final TopicPartitionRegInfo topicPartitionRegInfo = new TopicPartitionRegInfo(topic, partition, offset);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(url,
                new OffsetCommand(topic, this.consumerConfig.getGroup(), partition.getPartition(), offset,
                    Integer.MIN_VALUE), this.consumerConfig.getFetchTimeoutInMills(), TimeUnit.MILLISECONDS))
                    .andReturn(new BooleanCommand(HttpStatus.Success, String.valueOf(offset), Integer.MIN_VALUE));
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        assertEquals(offset, this.consumer.offset(new FetchRequest(broker, delay, topicPartitionRegInfo, maxSize)));
        this.mocksControl.verify();
    }


    @Test
    public void testGetFailed() throws Exception {
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final byte[] data = "hello".getBytes();
        final String url = "meta://localhost:0";
        final MessageIterator messageIterator = new MessageIterator(topic, data);

        this.producerZooKeeper.publishTopic(topic, this.consumer);
        EasyMock.expectLastCall();
        EasyMock.expect(this.remotingClient.isConnected(url)).andReturn(true);
        EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(url);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(url,
                new GetCommand(topic, this.consumerConfig.getGroup(), partition.getPartition(), offset, maxSize,
                    Integer.MIN_VALUE), 10000, TimeUnit.MILLISECONDS)).andReturn(
                        new BooleanCommand(500, "test error", Integer.MIN_VALUE));
        this.mocksControl.replay();
        try {
            OpaqueGenerator.resetOpaque();
            assertEquals(messageIterator, this.consumer.get(topic, partition, offset, maxSize));
            fail();
        }
        catch (final MetaClientException e) {

        }
        this.mocksControl.verify();
    }


    @Test
    public void testGetNotFound() throws Exception {
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final String url = "meta://localhost:0";

        this.producerZooKeeper.publishTopic(topic, this.consumer);
        EasyMock.expectLastCall();
        EasyMock.expect(this.remotingClient.isConnected(url)).andReturn(true);
        EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(url);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(url,
                new GetCommand(topic, this.consumerConfig.getGroup(), partition.getPartition(), offset, maxSize,
                    Integer.MIN_VALUE), 10000, TimeUnit.MILLISECONDS)).andReturn(
                        new BooleanCommand(404, "not found", Integer.MIN_VALUE));
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        assertNull(this.consumer.get(topic, partition, offset, maxSize));
        this.mocksControl.verify();
    }

}