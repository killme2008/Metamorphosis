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

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.consumer.SimpleFetchManager.FetchRequestRunner;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;


public class SimpleFetchManagerUnitTest {
    private SimpleFetchManager fetchManager;
    private ConsumerConfig consumerConfig;
    private InnerConsumer consumer;



    @Before
    public void setUp() {
        this.consumerConfig = new ConsumerConfig();
        this.consumer = EasyMock.createMock(InnerConsumer.class);
        EasyMock.makeThreadSafe(this.consumer, true);
        this.fetchManager = new SimpleFetchManager(this.consumerConfig, this.consumer);
        this.fetchManager.resetFetchState();
        this.fetchManager.setMessageIdCache(new ConcurrentLRUHashMap());
        // this.fetchManager.startFetchRunner();
    }


    @Test
    public void testProcessRequestNormal() throws Exception {

        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final int msgId = 1111;
        final byte[] data =
                MessageUtils.makeMessageBuffer(msgId,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        final AtomicReference<Message> msg = new AtomicReference<Message>();
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                msg.set(message);
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);

        final FetchRequest newRequest =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset + data.length, msgId),
                    maxSize);

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals("hello", new String(msg.get().getData()));
        assertEquals(msgId, msg.get().getId());
        assertEquals(topic, msg.get().getTopic());
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestNormalWithFilter() throws Exception {

        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final int msgId = 1111;
        final byte[] data =
                MessageUtils.makeMessageBuffer(msgId,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        final AtomicReference<Message> msg = new AtomicReference<Message>();
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                msg.set(message);
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        EasyMock.expect(this.consumer.getMessageFilter(topic)).andReturn(new ConsumerMessageFilter() {

            @Override
            public boolean accept(String group, Message message) {
                return true;
            }
        });
        EasyMock.expect(this.consumer.getConsumerConfig()).andReturn(new ConsumerConfig("test"));

        final FetchRequest newRequest =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset + data.length, msgId),
                    maxSize);

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals("hello", new String(msg.get().getData()));
        assertEquals(msgId, msg.get().getId());
        assertEquals(topic, msg.get().getTopic());
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestWithMessageFilterNotAccept() throws Exception {

        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final int msgId = 1111;
        final byte[] data =
                MessageUtils.makeMessageBuffer(msgId,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        final AtomicReference<Message> msg = new AtomicReference<Message>();
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                fail();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        // Don't accept all messages.
        EasyMock.expect(this.consumer.getMessageFilter(topic)).andReturn(new ConsumerMessageFilter() {

            @Override
            public boolean accept(String group, Message message) {
                // always return false.
                return false;
            }
        });
        EasyMock.expect(this.consumer.getConsumerConfig()).andReturn(new ConsumerConfig("test"));
        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertNull(msg.get());
    }


    @Test
    public void testProcessRequestWithMessageFilterThrowException() throws Exception {

        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final int msgId = 1111;
        final byte[] data =
                MessageUtils.makeMessageBuffer(msgId,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        final AtomicReference<Message> msg = new AtomicReference<Message>();
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                fail();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        // Don't accept all messages.
        EasyMock.expect(this.consumer.getMessageFilter(topic)).andReturn(new ConsumerMessageFilter() {

            @Override
            public boolean accept(String group, Message message) {
                throw new RuntimeException();
            }
        });
        EasyMock.expect(this.consumer.getConsumerConfig()).andReturn(new ConsumerConfig("test"));

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertNull(msg.get());
    }


    public static ByteBuffer makeInvalidMessageBuffer(final long msgId, final PutCommand req) {
        // message length + checksum + id +flag + data
        final ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 8 + 4 + req.getData().length);
        buffer.putInt(req.getData().length);
        // invalid checksum
        buffer.putInt(CheckSum.crc32(req.getData()) + 1);
        buffer.putLong(msgId);
        buffer.putInt(req.getFlag());
        buffer.put(req.getData());
        buffer.flip();
        return buffer;
    }


    private void mockFilterAndGroup(String topic) {
        EasyMock.expect(this.consumer.getMessageFilter(topic)).andReturn(null);
        EasyMock.expect(this.consumer.getConsumerConfig()).andReturn(new ConsumerConfig("test"));
    }


    @Test
    public void testProcessRequestInvalidMessage() throws Exception {

        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final int msgId = 1111;
        final byte[] data =
                makeInvalidMessageBuffer(msgId,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                fail();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);
        // query offset
        final long newOffset = 13;
        EasyMock.expect(this.consumer.offset(request)).andReturn(newOffset);
        // Use new offset
        final FetchRequest newRequest =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, newOffset, -1), maxSize);

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestRetryTooMany() throws Exception {

        // 最大尝试一次，则必然超过
        this.consumerConfig.setMaxFetchRetries(0);

        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final byte[] data =
                MessageUtils.makeMessageBuffer(1111,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);
        request.incrementRetriesAndGet();
        assertEquals(1, request.getRetries());
        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                fail();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);
        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();
        // EasyMock.expect(this.fetchManager.isRetryTooMany(request)).andReturn(true);
        final Message message = new Message(topic, "hello".getBytes());
        MessageAccessor.setId(message, 1111);
        this.consumer.appendCouldNotProcessMessage(message);
        EasyMock.expectLastCall();

        // offset递增，因为跳过当前消息
        final FetchRequest newRequest =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset + data.length, 1111),
                    maxSize);

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        // retry被重设
        assertEquals(0, request.getRetries());
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestDelayed() throws Exception {
        // this.mockConsumerReInitializeFetchManager();
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();
        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(null);
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                fail();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);

        final FetchRequest newRequest =
                new FetchRequest(broker, this.consumerConfig.getMaxDelayFetchTimeInMills() / 10,
                    new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestException() throws Exception {
        // this.mockConsumerReInitializeFetchManager();
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final byte[] data =
                MessageUtils.makeMessageBuffer(1111,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                throw new RuntimeException("A stupid bug");
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);

        final FetchRequest newRequest =
                new FetchRequest(broker, this.consumerConfig.getMaxDelayFetchTimeInMills() / 10,
                    new TopicPartitionRegInfo(topic, partition, offset, 1111), maxSize);
        newRequest.incrementRetriesAndGet();

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestMessageRollbackOnly() throws Exception {
        // this.mockConsumerReInitializeFetchManager();
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final byte[] data =
                MessageUtils.makeMessageBuffer(1111,
                    new PutCommand(topic, partition.getPartition(), "hello".getBytes(), null, 0, 0)).array();
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();

        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, data));
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                System.out.println("Rollback current message");
                message.setRollbackOnly();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);

        final FetchRequest newRequest =
                new FetchRequest(broker, this.consumerConfig.getMaxDelayFetchTimeInMills() / 10,
                    new TopicPartitionRegInfo(topic, partition, offset, 1111), maxSize);
        newRequest.incrementRetriesAndGet();

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }


    @Test
    public void testProcessRequestDelayed_IncreaseMaxSize() throws Exception {
        // this.mockConsumerReInitializeFetchManager();
        // 保证超过尝试次数
        this.consumerConfig.setMaxIncreaseFetchDataRetries(0);
        final String topic = "topic1";
        final int maxSize = 1024;
        final Partition partition = new Partition("0-0");
        final long offset = 12;
        final Broker broker = new Broker(0, "meta://localhost:0");
        final FetchRequest request =
                new FetchRequest(broker, 0, new TopicPartitionRegInfo(topic, partition, offset), maxSize);
        // 递增次数，超过0次
        request.incrementRetriesAndGet();

        final FetchRequestRunner runner = this.fetchManager.new FetchRequestRunner();
        EasyMock.expect(this.consumer.fetch(request, -1, null)).andReturn(new MessageIterator(topic, new byte[10]));
        EasyMock.expect(this.consumer.getMessageListener(topic)).andReturn(new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                fail();
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        this.mockFilterAndGroup(topic);
        final int newMaxSize = maxSize * 2;
        final FetchRequest newRequest =
                new FetchRequest(broker, this.consumerConfig.getMaxDelayFetchTimeInMills() / 10,
                    new TopicPartitionRegInfo(topic, partition, offset), newMaxSize);
        newRequest.incrementRetriesAndGet();
        newRequest.incrementRetriesAndGet();

        EasyMock.replay(this.consumer);
        runner.processRequest(request);
        EasyMock.verify(this.consumer);
        assertEquals(newRequest, this.fetchManager.takeFetchRequest());
    }
}