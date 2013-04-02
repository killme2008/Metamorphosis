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
package com.taobao.metamorphosis.client.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.exception.TransactionInProgressException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.TransactionCommand;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionInfo;
import com.taobao.metamorphosis.transaction.TransactionInfo.TransactionType;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageFlagUtils;
import com.taobao.metamorphosis.utils.MessageUtils;


public class SimpleMessageProducerUnitTest {
    private SimpleMessageProducer producer;
    private ProducerZooKeeper producerZooKeeper;
    private PartitionSelector partitionSelector;
    private RemotingClientWrapper remotingClient;
    private IMocksControl mocksControl;
    private final String sessionId = "testSession";


    @Before
    public void setUp() {
        this.mocksControl = EasyMock.createControl();
        this.producerZooKeeper = this.mocksControl.createMock(ProducerZooKeeper.class);
        this.partitionSelector = new RoundRobinPartitionSelector();
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.producer =
                new SimpleMessageProducer(null, this.remotingClient, this.partitionSelector, this.producerZooKeeper,
                    this.sessionId);
        OpaqueGenerator.resetOpaque();
    }


    @Test
    public void testSetTransactionRequestTimeout() {
        assertEquals(5000L, this.producer.getTransactionRequestTimeoutInMills());
        this.producer.setTransactionRequestTimeout(3, TimeUnit.SECONDS);
        assertEquals(3000L, this.producer.getTransactionRequestTimeoutInMills());
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


    @Test
    public void testSendMessageNormal_NoPartitions() throws Exception {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        EasyMock.expect(this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector)).andReturn(null);
        this.mocksControl.replay();
        try {
            this.producer.sendMessage(message);
            fail();
        }
        catch (final MetaClientException e) {

        }
        this.mocksControl.verify();
    }


    @Test
    public void testSendMessageNormal_NoBroker() throws Exception {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        final Partition partition = new Partition("0-0");
        EasyMock.expect(this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector)).andReturn(
            partition);
        EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(null);
        this.mocksControl.replay();
        try {
            this.producer.sendMessage(new Message(topic, data));
            fail();
        }
        catch (final MetaClientException e) {
            // e.printStackTrace();
        }
        this.mocksControl.verify();
    }


    @Test
    public void testSendOrderedMessageServerError() throws Exception {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        final String url = "meta://localhost:0";
        final Partition partition = new Partition("0-0");
        // ≥¢ ‘÷ÿ∏¥3¥Œ
        EasyMock.expect(this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector)).andReturn(
            partition);// .times(3);
        EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(url);// .times(3);
        OpaqueGenerator.resetOpaque();
        final int flag = MessageFlagUtils.getFlag(null);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(url,
                new PutCommand(topic, partition.getPartition(), data, flag, CheckSum.crc32(data), null,
                    Integer.MIN_VALUE), 3000, TimeUnit.MILLISECONDS)).andReturn(
                        new BooleanCommand(500, "server error", Integer.MIN_VALUE));
        // EasyMock.expect(
        // this.remotingClient.invokeToGroup(url, new PutCommand(topic,
        // partition.getPartition(), data, null, flag,
        // Integer.MIN_VALUE + 1), 3000, TimeUnit.MILLISECONDS)).andReturn(
        // new BooleanCommand(Integer.MIN_VALUE, 500, "server error"));
        // EasyMock.expect(
        // this.remotingClient.invokeToGroup(url, new PutCommand(topic,
        // partition.getPartition(), data, null, flag,
        // Integer.MIN_VALUE + 2), 3000, TimeUnit.MILLISECONDS)).andReturn(
        // new BooleanCommand(Integer.MIN_VALUE, 500, "server error"));
        this.mocksControl.replay();
        assertEquals(0, message.getId());
        final SendResult sendResult = this.producer.sendMessage(message);

        this.mocksControl.verify();
        assertFalse(sendResult.isSuccess());
        assertEquals(-1, sendResult.getOffset());
        assertNull(sendResult.getPartition());
        assertEquals("server error", sendResult.getErrorMessage());
    }


    @Test
    public void testSendMessageInterrupted() throws Exception {
        boolean interrupted = false;
        try {
            final String topic = "topic1";
            final byte[] data = "hello".getBytes();
            final Message message = new Message(topic, data);
            final String url = "meta://localhost:0";
            final Partition partition = new Partition("0-0");
            EasyMock.expect(this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector)).andReturn(
                partition);
            EasyMock.expect(this.producerZooKeeper.selectBroker(topic, partition)).andReturn(url);
            OpaqueGenerator.resetOpaque();
            final int flag = MessageFlagUtils.getFlag(null);
            EasyMock.expect(
                this.remotingClient.invokeToGroup(url, new PutCommand(topic, partition.getPartition(), data, flag,
                    CheckSum.crc32(data), null, Integer.MIN_VALUE), 3000, TimeUnit.MILLISECONDS)).andThrow(
                        new InterruptedException());
            this.mocksControl.replay();
            this.producer.sendMessage(message);
        }
        catch (final InterruptedException e) {
            interrupted = true;
        }
        this.mocksControl.verify();
        assertTrue(interrupted);
    }


    @Test
    public void testBeginTransactionCommit() throws Exception {
        this.producer.beginTransaction();
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(new LocalTransactionId(this.sessionId, 1),
            this.sessionId, TransactionType.BEGIN), null);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(new LocalTransactionId(this.sessionId, 1),
            this.sessionId, TransactionType.COMMIT_ONE_PHASE), null);
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        this.producer.beforeSendMessageFirstTime(serverUrl);
        assertTrue(this.producer.isInTransaction());
        this.producer.commit();
        this.mocksControl.verify();
        assertFalse(this.producer.isInTransaction());
    }


    @Test
    public void testBeginTransactionRollback() throws Exception {
        this.producer.beginTransaction();
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(new LocalTransactionId(this.sessionId, 1),
            this.sessionId, TransactionType.BEGIN), null);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(new LocalTransactionId(this.sessionId, 1),
            this.sessionId, TransactionType.ROLLBACK), null);
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        this.producer.beforeSendMessageFirstTime(serverUrl);
        assertTrue(this.producer.isInTransaction());
        this.producer.rollback();
        this.mocksControl.verify();
        assertFalse(this.producer.isInTransaction());
    }


    @Test(expected = TransactionInProgressException.class)
    public void testBeginTwice() throws Exception {
        this.producer.beginTransaction();
        this.producer.beginTransaction();
        fail();
    }


    @Test(expected = MetaClientException.class)
    public void testCommitUnBegin() throws Exception {
        this.producer.commit();
        fail();
    }


    @Test(expected = MetaClientException.class)
    public void tesRollbackUnBegin() throws Exception {
        this.producer.rollback();
        fail();
    }


    private void mockInvokeSuccess(final String serverUrl, final TransactionInfo info, final String result)
            throws InterruptedException, TimeoutException, NotifyRemotingException {
        EasyMock.expect(
            this.remotingClient.invokeToGroup(serverUrl, new TransactionCommand(info, OpaqueGenerator.getNextOpaque()),
                5000L, TimeUnit.MILLISECONDS)).andReturn(new BooleanCommand(HttpStatus.Success, result, 0));
    }


    private void mockIsConnected(final String serverUrl, final boolean rt) {
        EasyMock.expect(this.remotingClient.isConnected(serverUrl)).andReturn(rt).anyTimes();
    }


    @Test
    public void testEncodeData_NoAttribute() {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        final byte[] encoded = MessageUtils.encodePayload(message);
        assertEquals("hello", new String(encoded));
    }


    @Test
    public void testEncodeData_HasAttribute() throws Exception {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final String attribute = "attribute";
        final Message message = new Message(topic, data, attribute);
        final byte[] encoded = MessageUtils.encodePayload(message);
        assertEquals(4 + attribute.length() + data.length, encoded.length);
        assertEquals(attribute.length(), MessageUtils.getInt(0, encoded));
        assertEquals(attribute, new String(encoded, 4, attribute.length()));
        assertEquals("hello", new String(encoded, 4 + attribute.length(), data.length));
    }


    @Test
    public void testEncodeData_EmptyAttribute() throws Exception {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final String attribute = "";
        final Message message = new Message(topic, data, attribute);
        final byte[] encoded = MessageUtils.encodePayload(message);
        assertEquals(4 + attribute.length() + data.length, encoded.length);
        assertEquals(attribute.length(), MessageUtils.getInt(0, encoded));
        assertEquals(attribute, new String(encoded, 4, attribute.length()));
        assertEquals("hello", new String(encoded, 4 + attribute.length(), data.length));
    }

}