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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper;
import com.taobao.metamorphosis.client.producer.RoundRobinPartitionSelector;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageFlagUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-8-24 ÏÂÎç5:41:23
 */

public class OrderedMessageProducerTest {
    private OrderedMessageProducer producer;
    private ProducerZooKeeper producerZooKeeper;
    private PartitionSelector partitionSelector;
    private RemotingClientWrapper remotingClient;
    private OrderedLocalMessageStorageManager localMessageStorageManager;
    private IMocksControl mocksControl;


    @Before
    public void setUp() {
        this.mocksControl = EasyMock.createControl();
        this.producerZooKeeper = this.mocksControl.createMock(ProducerZooKeeper.class);
        this.partitionSelector = new RoundRobinPartitionSelector();
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.localMessageStorageManager = this.mocksControl.createMock(OrderedLocalMessageStorageManager.class);
        this.producer =
                new OrderedMessageProducer(null, this.remotingClient, this.partitionSelector, this.producerZooKeeper,
                    null, this.localMessageStorageManager);
    }


    @Test
    public void testSendOrderedMessage() throws Exception {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        final String url = "meta://localhost:0";
        final Partition partition = new Partition("0-0");
        EasyMock.expect(this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector))
        .andReturn(partition).times(2);
        EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(url);
        EasyMock.expect(this.localMessageStorageManager.getMessageCount(topic, partition)).andReturn(0);
        OpaqueGenerator.resetOpaque();
        final int flag = MessageFlagUtils.getFlag(null);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(url, new PutCommand(topic, partition.getPartition(), data, flag, CheckSum.crc32(data),
                null, Integer.MIN_VALUE), 3000, TimeUnit.MILLISECONDS)).andReturn(
                    new BooleanCommand(200, "1111 1 1024", Integer.MIN_VALUE));
        this.mocksControl.replay();
        assertEquals(0, message.getId());
        final SendResult sendResult = this.producer.sendMessage(message);

        this.mocksControl.verify();
        assertTrue(sendResult.isSuccess());
        assertEquals(1024, sendResult.getOffset());
        assertEquals(1, sendResult.getPartition().getPartition());
        assertEquals(0, sendResult.getPartition().getBrokerId());
        assertEquals(1111, message.getId());
    }


    @Test
    public void testSendInvalidMessage() throws Exception {
        try {
            this.producer.sendMessage(null);
            fail();
        }
        catch (final InvalidMessageException e) {
            assertEquals("Null message", e.getMessage());
        }
        try {
            this.producer.sendMessage(new Message(null, "hello".getBytes()));
            fail();
        }
        catch (final InvalidMessageException e) {
            assertEquals("Blank topic", e.getMessage());
        }
        try {
            this.producer.sendMessage(new Message("topic", null));
            fail();
        }
        catch (final InvalidMessageException e) {
            assertEquals("Null data", e.getMessage());
        }
    }
}