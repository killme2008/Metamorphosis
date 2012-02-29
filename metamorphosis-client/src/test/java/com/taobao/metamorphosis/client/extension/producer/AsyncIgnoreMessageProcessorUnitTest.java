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

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.producer.MessageRecoverManager.MessageRecoverer;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-12-29 ÏÂÎç2:00:59
 */

public class AsyncIgnoreMessageProcessorUnitTest {
    AsyncIgnoreMessageProcessor processor;
    MessageRecoverManager messageRecoverManager;
    IMocksControl mocksControl;


    @Before
    public void setUp() throws Exception {
        processor = new AsyncIgnoreMessageProcessor(new MetaClientConfig(), new MessageRecoverer() {

            @Override
            public void handle(Message msg) throws Exception {

            }
        });
        mocksControl = EasyMock.createControl();
        messageRecoverManager = mocksControl.createMock(MessageRecoverManager.class);
    }


    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testAsyncIgnoreMessageProcessor() {
        new AsyncIgnoreMessageProcessor(new MetaClientConfig(), new MessageRecoverer() {

            @Override
            public void handle(Message msg) throws Exception {

            }
        });
    }


    @Test
    public void testHandle() throws Exception {
        String topic = "test-topic";
        Partition partition = new Partition(0, 0);
        Message message = new Message(topic, "test".getBytes());
        MessageAccessor.setPartition(message, partition);
        processor.setStorageManager(messageRecoverManager);
        EasyMock.expect(messageRecoverManager.getMessageCount(topic, partition)).andReturn(499999);
        messageRecoverManager.append(message, partition);
        EasyMock.expectLastCall();
        mocksControl.replay();

        Assert.assertTrue(processor.handle(message));

        mocksControl.verify();
    }


    @Test
    public void testHandle_unknowPartition() throws Exception {
        String topic = "test-topic";
        Message message = new Message(topic, "test".getBytes());
        processor.setStorageManager(messageRecoverManager);
        EasyMock.expect(messageRecoverManager.getMessageCount(topic, Partition.RandomPartiton)).andReturn(499999);
        messageRecoverManager.append(message, Partition.RandomPartiton);
        EasyMock.expectLastCall();
        mocksControl.replay();

        Assert.assertTrue(processor.handle(message));

        mocksControl.verify();
    }


    @Test
    public void testHandle_haveTooManyLocalMessages() throws Exception {
        String topic = "test-topic";
        Message message = new Message(topic, "test".getBytes());
        processor.setStorageManager(messageRecoverManager);
        EasyMock.expect(messageRecoverManager.getMessageCount(topic, Partition.RandomPartiton)).andReturn(500001);
        EasyMock.expectLastCall();
        mocksControl.replay();

        Assert.assertFalse(processor.handle(message));

        mocksControl.verify();
    }
}