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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.producer.MessageRecoverManager.MessageRecoverer;
import com.taobao.metamorphosis.client.extension.producer.OrderedLocalMessageStorageManager;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 
 * @author 无花
 * @since 2011-8-9 上午9:43:16
 */

public class OrderedLocalMessageStorageManagerUnitTest {

    private OrderedLocalMessageStorageManager localMessageStorageManager;
    private final Partition partition1 = new Partition("0-0");
    private final Partition partition2 = new Partition("0-1");


    @Before
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(new File(OrderedLocalMessageStorageManager.DEFAULT_META_LOCALMESSAGE_PATH));
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        this.localMessageStorageManager = new OrderedLocalMessageStorageManager(metaClientConfig);

    }


    @After
    public void tearDown() {
        this.localMessageStorageManager.shutdown();
    }


    @Test
    public void testAppendGetCountRecover() throws Exception {
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        this.localMessageStorageManager.append(this.createDefaultMessage(topic1), this.partition1);
        this.localMessageStorageManager.append(this.createDefaultMessage(topic1), this.partition2);
        this.localMessageStorageManager.append(this.createDefaultMessage(topic1), Partition.RandomPartiton);
        this.localMessageStorageManager.append(this.createDefaultMessage(topic2), this.partition1);

        System.out.println(this.localMessageStorageManager.getMessageCount(topic1, this.partition1));
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic1, this.partition1) == 1);
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic1, this.partition2) == 1);
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic2, this.partition1) == 1);

        final BlockingQueue<Message> queue1 = new ArrayBlockingQueue<Message>(10);
        final BlockingQueue<Message> queue2 = new ArrayBlockingQueue<Message>(10);

        final MessageRecoverer recoverer1 = this.createRecover(topic1, queue1);
        final MessageRecoverer recoverer2 = this.createRecover(topic2, queue2);

        this.localMessageStorageManager.recover(topic1, this.partition1, recoverer1);
        // again,no problem
        this.localMessageStorageManager.recover(topic1, this.partition1, recoverer1);
        this.localMessageStorageManager.recover(topic1, this.partition2, recoverer1);

        this.localMessageStorageManager.recover(topic2, this.partition1, recoverer2);
        // again,no problem
        this.localMessageStorageManager.recover(topic1, this.partition1, recoverer1);

        while (queue1.size() < 3) {
            Thread.sleep(200);
        }
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic1, Partition.RandomPartiton) == 0);
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic1, this.partition1) == 0);
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic1, this.partition2) == 0);
        Thread.sleep(200);
        Assert.assertEquals(3, queue1.size());

        while (queue2.size() < 1) {
            Thread.sleep(200);
        }
        Assert.assertTrue(this.localMessageStorageManager.getMessageCount(topic2, this.partition1) == 0);
        Thread.sleep(200);
        Assert.assertTrue(queue2.size() == 1);

        // 恢复完毕后再调用recover
        queue2.clear();
        this.localMessageStorageManager.recover(topic2, this.partition1, recoverer2);
        Thread.sleep(200);
        Assert.assertTrue(queue2.size() == 0);

    }


    private MessageRecoverer createRecover(final String topic, final BlockingQueue<Message> queue) {
        final MessageRecoverer recoverer1 = new MessageRecoverer() {

            @Override
            public void handle(final Message msg) throws Exception {
                Assert.assertEquals(topic, msg.getTopic());
                Assert.assertEquals("hello", new String(msg.getData()));
                System.out.println("recover msg：" + msg);
                queue.offer(msg);
            }
        };
        return recoverer1;
    }


    private Message createDefaultMessage(final String topic) {
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        return message;
    }

}