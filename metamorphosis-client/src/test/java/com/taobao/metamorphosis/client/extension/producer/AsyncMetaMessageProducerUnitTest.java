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
package com.taobao.metamorphosis.client.extension.producer;

import java.util.concurrent.TimeUnit;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.extension.producer.AsyncMessageProducer.IgnoreMessageProcessor;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper;
import com.taobao.metamorphosis.client.producer.RoundRobinPartitionSelector;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-12-29 ÏÂÎç2:31:03
 */

public class AsyncMetaMessageProducerUnitTest {
    private AsyncMetaMessageProducer producer;
    private ProducerZooKeeper producerZooKeeper;
    private PartitionSelector partitionSelector;
    private RemotingClientWrapper remotingClient;
    private MetaMessageSessionFactory messageSessionFactory;
    private IgnoreMessageProcessor processor;
    private IMocksControl mocksControl;
    private final String sessionId = "testSession";


    @Before
    public void setUp() throws Exception {
        this.mocksControl = EasyMock.createControl();
        this.producerZooKeeper = this.mocksControl.createMock(ProducerZooKeeper.class);
        this.partitionSelector = new RoundRobinPartitionSelector();
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.messageSessionFactory = this.mocksControl.createMock(MetaMessageSessionFactory.class);
        this.processor = this.mocksControl.createMock(IgnoreMessageProcessor.class);
        this.producer =
                new AsyncMetaMessageProducer(messageSessionFactory, this.remotingClient, this.partitionSelector,
                    this.producerZooKeeper, this.sessionId, 20000, processor);
        OpaqueGenerator.resetOpaque();
    }


    @Test
    public void testAsyncMetaMessageProducer() {
        AsyncMetaMessageProducer producer =
                new AsyncMetaMessageProducer(messageSessionFactory, this.remotingClient, this.partitionSelector,
                    this.producerZooKeeper, this.sessionId, 20000, processor);
        Assert.assertTrue(producer.getIgnoreMessageProcessor() == processor);

        EasyMock.expect(messageSessionFactory.getMetaClientConfig()).andReturn(new MetaClientConfig());
        mocksControl.replay();
        producer =
                new AsyncMetaMessageProducer(messageSessionFactory, this.remotingClient, this.partitionSelector,
                    this.producerZooKeeper, this.sessionId, 20000, null);
        Assert.assertTrue(producer.getIgnoreMessageProcessor() instanceof AsyncIgnoreMessageProcessor);
        mocksControl.verify();
    }


    @Test
    public void testAsyncSendMessageMessage() throws MetaClientException, NotifyRemotingException {
        String topic = "test-topic";
        Partition partition = new Partition(0, 0);
        String serverUrl = "meta://127.0.0.1:8123";
        Message message = new Message(topic, "test".getBytes());
        EasyMock.resetToNice(remotingClient);
        EasyMock.expect(producerZooKeeper.selectPartition(topic, message, partitionSelector)).andReturn(partition);
        EasyMock.expect(producerZooKeeper.selectBroker(topic, partition)).andReturn(serverUrl);
        OpaqueGenerator.resetOpaque();
        mocksControl.replay();
        producer.asyncSendMessage(message, 10000, TimeUnit.MILLISECONDS);
        mocksControl.verify();
    }


    @Test
    public void testAsyncSendMessageMessageLongTimeUnit_producerShutdown() throws MetaClientException {
        Message message = new Message("test-topic", "test".getBytes());
        messageSessionFactory.removeChild(producer);
        EasyMock.expectLastCall();
        EasyMock.replay();
        producer.shutdown();
        producer.asyncSendMessage(message, 10000, TimeUnit.MILLISECONDS);
        EasyMock.verify();
    }


    @Test
    public void testAsyncSendMessageMessageLongTimeUnit_InvalidMessage() throws MetaClientException {
        producer.asyncSendMessage(new Message("", "test".getBytes()), 10000, TimeUnit.MILLISECONDS);
        producer.asyncSendMessage(new Message("  ", "test".getBytes()), 10000, TimeUnit.MILLISECONDS);
        producer.asyncSendMessage(new Message(null, "test".getBytes()), 10000, TimeUnit.MILLISECONDS);
    }


    @Test
    public void testAsyncSendMessageMessageLongTimeUnit_MetaClientException() throws Exception {

        String topic = "test-topic";
        Message message = new Message(topic, "test".getBytes());

        EasyMock.expect(producerZooKeeper.selectPartition(topic, message, partitionSelector)).andReturn(null);
        EasyMock.expect(processor.handle(message)).andReturn(true);
        mocksControl.replay();
        producer.asyncSendMessage(message, 10000, TimeUnit.MILLISECONDS);
        mocksControl.verify();
    }


    @Test
    public void testSetIgnoreMessageProcessor() {
        IgnoreMessageProcessor processor = new IgnoreMessageProcessor() {

            @Override
            public boolean handle(Message message) throws Exception {
                return true;
            }
        };
        producer.setIgnoreMessageProcessor(processor);
        Assert.assertTrue(processor == producer.getIgnoreMessageProcessor());
    }


    @Test
    public void testHandle() throws Exception {
        String topic = "test-topic";
        Partition partition = new Partition(0, 0);
        String serverUrl = "meta://127.0.0.1:8123";
        Message message = new Message(topic, "test".getBytes());
        EasyMock.resetToNice(remotingClient);
        EasyMock.expect(producerZooKeeper.selectPartition(topic, message, partitionSelector)).andReturn(partition);
        EasyMock.expect(producerZooKeeper.selectBroker(topic, partition)).andReturn(serverUrl);
        OpaqueGenerator.resetOpaque();
        mocksControl.replay();
        producer.handle(message);
        mocksControl.verify();
    }

}