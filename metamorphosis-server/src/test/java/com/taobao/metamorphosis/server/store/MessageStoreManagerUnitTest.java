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
package com.taobao.metamorphosis.server.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.IdWorker;


public class MessageStoreManagerUnitTest {

    private MessageStoreManager messageStoreManager;
    private MetaConfig metaConfig;


    @Before
    public void setUp() throws Exception {
        this.metaConfig = new MetaConfig();
        final String topic = "MessageStoreManagerUnitTest";
        this.metaConfig.getTopics().add(topic);
        final TopicConfig topicConfig = new TopicConfig(topic, this.metaConfig);
        topicConfig.setDeletePolicy("delete,10s");
        topicConfig.setDeleteWhen("0/1 * * * * ?");
        this.metaConfig.getTopicConfigMap().put(topic, topicConfig);
        FileUtils.deleteDirectory(new File(this.metaConfig.getDataPath()));
        this.messageStoreManager = new MessageStoreManager(this.metaConfig, null);
    }


    @After
    public void tearDown() throws Exception {
        this.messageStoreManager.dispose();
    }


    @Test
    public void testGetOrCreateMessageStore() throws Exception {
        final String topic = "MessageStoreManagerUnitTest";
        final int partition = 0;
        final File dir = new File(this.metaConfig.getDataPath() + File.separator + topic + "-" + partition);
        assertFalse(dir.exists());
        assertNull(this.messageStoreManager.getMessageStore(topic, partition));
        final MessageStore store1 = this.messageStoreManager.getOrCreateMessageStore(topic, partition);
        assertTrue(dir.exists());
        final MessageStore store2 = this.messageStoreManager.getMessageStore(topic, partition);
        assertSame(store1, store2);
    }


    @Test
    public void testGetOrCreateMessageStore_withOffset() throws Exception {
        final String topic = "MessageStoreManagerUnitTest";
        final int partition = 0;
        final File dir = new File(this.metaConfig.getDataPath() + File.separator + topic + "-" + partition);
        assertFalse(dir.exists());
        assertNull(this.messageStoreManager.getMessageStore(topic, partition));
        final MessageStore store1 = this.messageStoreManager.getOrCreateMessageStore(topic, partition, 2048);
        assertTrue(dir.exists());
        final MessageStore store2 = this.messageStoreManager.getMessageStore(topic, partition);
        assertSame(store1, store2);
        assertEquals(2048, store2.getMinOffset());

        final IdWorker idWorker = new IdWorker(0);
        final PutCommand cmd1 = new PutCommand(topic, partition, "hello".getBytes(), null, 0, 0);
        final PutCommand cmd2 = new PutCommand(topic, partition, "world".getBytes(), null, 0, 0);
        store1.append(idWorker.nextId(), cmd1, new AppendCallback() {

            @Override
            public void appendComplete(final Location location) {
                assertEquals(2048, location.getOffset());

            }
        });
        store1.flush();// flush一次
        final long size = store1.getSegments().last().size();
        store1.append(idWorker.nextId(), cmd2, new AppendCallback() {
            @Override
            public void appendComplete(final Location location) {
                assertEquals(2048 + size, location.getOffset());

            }
        });
        store1.flush();
        assertEquals(1, dir.listFiles().length);
        assertTrue(dir.listFiles()[0].exists());
        assertEquals(store1.nameFromOffset(2048), dir.listFiles()[0].getName());

        store1.close();
        for (final File file : dir.listFiles()) {
            file.delete();
        }
    }


    @Test
    public void testChooseRandomPartition() {
        assertEquals(0, this.messageStoreManager.chooseRandomPartition("MessageStoreManagerUnitTest"));
        assertEquals(0, this.messageStoreManager.chooseRandomPartition("MessageStoreManagerUnitTest"));
        assertEquals(0, this.messageStoreManager.chooseRandomPartition("MessageStoreManagerUnitTest"));

        this.metaConfig.setNumPartitions(10);
        for (int i = 0; i < 100; i++) {
            assertTrue(this.messageStoreManager.chooseRandomPartition("MessageStoreManagerUnitTest") < 10);
        }
    }


    @Test
    public void testGetNumPartitions() {
        assertEquals(1, this.messageStoreManager.getNumPartitions("MessageStoreManagerUnitTest"));
        assertEquals(1, this.messageStoreManager.getNumPartitions("MessageStoreManagerUnitTest"));

        final TopicConfig topicConfig = new TopicConfig("MessageStoreManagerUnitTest", this.metaConfig);
        topicConfig.setNumPartitions(9999);
        this.metaConfig.getTopicConfigMap().put("MessageStoreManagerUnitTest", topicConfig);
        assertEquals(9999, this.messageStoreManager.getNumPartitions("MessageStoreManagerUnitTest"));
        assertEquals(9999, this.messageStoreManager.getNumPartitions("MessageStoreManagerUnitTest"));
    }


    @Test
    public void testInit() throws Exception {
        this.testInit0(false);
    }


    @Test
    public void testInitInParallel() throws Exception {
        this.testInit0(true);
    }


    private void testInit0(boolean inParallel) throws IOException, InterruptedException {
        final String topic = "MessageStoreManagerUnitTest";
        IdWorker idWorker = new IdWorker(0);
        final TopicConfig topicConfig = new TopicConfig(topic, this.metaConfig);
        topicConfig.setNumPartitions(10);
        this.metaConfig.getTopicConfigMap().put("MessageStoreManagerUnitTest", topicConfig);
        this.metaConfig.setLoadMessageStoresInParallel(inParallel);
        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getMessageStore(topic, i);
            Assert.assertNull(store);
        }
        final CountDownLatch latch = new CountDownLatch(1000);
        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getOrCreateMessageStore(topic, i);
            for (int j = 0; j < 100; j++) {
                final PutCommand cmd = new PutCommand(topic, i, new byte[1024], null, 0, 0);
                final long id = idWorker.nextId();
                store.append(id, cmd, new AppendCallback() {

                    @Override
                    public void appendComplete(Location location) {
                        if (location == Location.InvalidLocaltion) {
                            throw new IllegalStateException();
                        }
                        latch.countDown();

                    }
                });
            }
        }
        latch.await();
        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getMessageStore(topic, i);
            Assert.assertNotNull(store);
        }
        this.messageStoreManager.dispose();
        this.messageStoreManager = new MessageStoreManager(this.metaConfig, null);
        this.messageStoreManager.init();
        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getMessageStore(topic, i);
            Assert.assertNotNull(store);
        }
    }


    @Test
    public void testRunDeletePolicy() throws Exception {
        this.metaConfig.setMaxSegmentSize(1024);
        this.messageStoreManager.init();
        final String topic = "MessageStoreManagerUnitTest";
        final int partition = 0;
        final File dir = new File(this.metaConfig.getDataPath() + File.separator + topic + "-" + partition);
        assertFalse(dir.exists());
        assertNull(this.messageStoreManager.getMessageStore(topic, partition));
        final MessageStore store = this.messageStoreManager.getOrCreateMessageStore(topic, partition);

        final IdWorker idWorker = new IdWorker(0);
        final byte[] data = new byte[1024];
        final PutCommand cmd1 = new PutCommand(topic, partition, data, null, 0, 0);
        final PutCommand cmd2 = new PutCommand(topic, partition, data, null, 0, 0);
        store.append(idWorker.nextId(), cmd1, new AppendCallback() {

            @Override
            public void appendComplete(final Location location) {
                assertEquals(0, location.getOffset());

            }
        });
        store.flush();// flush一次
        store.append(idWorker.nextId(), cmd2, new AppendCallback() {
            @Override
            public void appendComplete(final Location location) {
                assertEquals(1044, location.getOffset());

            }
        });
        store.flush();
        System.out.println(store.getSegmentInfos().size());
        assertFalse(store.getSegmentInfos().isEmpty());
        assertEquals(3, store.getSegmentInfos().size());

        // Wait for 10 seconds
        Thread.sleep(15000);
        System.out.println(store.getSegmentInfos().size());
        assertEquals(1, store.getSegmentInfos().size());
    }


    @Test
    public void testIsLegalTopicWildChar() throws Exception {
        this.metaConfig.getTopics().add("TBCTU-*");
        this.tearDown();
        this.messageStoreManager = new MessageStoreManager(this.metaConfig, null);
        assertTrue(this.messageStoreManager.isLegalTopic("TBCTU-test"));
        assertTrue(this.messageStoreManager.isLegalTopic("TBCTU-2343"));
        assertTrue(this.messageStoreManager.isLegalTopic("TBCTU-TBCTU"));
        assertTrue(this.messageStoreManager.isLegalTopic("TBCTU-"));
        assertFalse(this.messageStoreManager.isLegalTopic("a-TBCTU-"));
        assertFalse(this.messageStoreManager.isLegalTopic("TCCTU-test"));
    }
}