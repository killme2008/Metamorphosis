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

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.MetaClientConfig;


public class RecoverStorageManagerUnitTest {

    private RecoverStorageManager recoverStorageManager;

    private SubscribeInfoManager subscribeInfoManager;


    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File(RecoverStorageManager.META_RECOVER_STORE_PATH));
        this.subscribeInfoManager = new SubscribeInfoManager();
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setRecoverMessageIntervalInMills(1000);
        this.recoverStorageManager = new RecoverStorageManager(metaClientConfig, this.subscribeInfoManager);
        this.recoverStorageManager.start(metaClientConfig);
    }


    @After
    public void tearDown() {
        this.recoverStorageManager.shutdown();
    }


    @Test
    public void testAppendShutdownLoadRecover() throws Exception {
        this.recoverStorageManager.shutdown();
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setRecoverMessageIntervalInMills(Integer.MAX_VALUE);
        this.recoverStorageManager = new RecoverStorageManager(metaClientConfig, this.subscribeInfoManager);
        this.recoverStorageManager.start(metaClientConfig);
        final String group = "dennis";
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(1024);
        this.subscribeInfoManager.subscribe("test", group, 1024 * 1024, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                queue.offer(message);
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        }, null);
        for (int i = 0; i < 100; i++) {
            final Message msg2 = new Message("test", ("hello" + i).getBytes());
            MessageAccessor.setId(msg2, i);
            this.recoverStorageManager.append(group, msg2);
        }
        this.recoverStorageManager.shutdown();

        // 重新启动，设置recover间隔为1秒
        metaClientConfig.setRecoverMessageIntervalInMills(1000);
        this.recoverStorageManager = new RecoverStorageManager(metaClientConfig, this.subscribeInfoManager);
        this.recoverStorageManager.start(metaClientConfig);
        while (queue.size() < 100) {
            Thread.sleep(1000);
        }
        for (final Message msg : queue) {
            assertEquals("hello" + msg.getId(), new String(msg.getData()));
        }
        assertEquals(0, this.recoverStorageManager.getOrCreateStore("test", group).size());

    }


    @Test
    public void testAppendRecover() throws Exception {
        final String group = "dennis";
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(1024);
        this.subscribeInfoManager.subscribe("test", group, 1024 * 1024, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                queue.offer(message);
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        }, null);
        for (int i = 0; i < 100; i++) {
            final Message msg2 = new Message("test", ("hello" + i).getBytes());
            MessageAccessor.setId(msg2, i);
            this.recoverStorageManager.append(group, msg2);
        }

        while (queue.size() < 100) {
            Thread.sleep(1000);
        }
        for (final Message msg : queue) {
            assertEquals("hello" + msg.getId(), new String(msg.getData()));
        }
        assertEquals(0, this.recoverStorageManager.getOrCreateStore("test", group).size());
    }


    @Test
    public void testAppendDupKey2() throws Exception {
        final Message msg1 = new Message("test", "hello".getBytes());
        MessageAccessor.setId(msg1, 1);
        this.recoverStorageManager.append("group", msg1);
        this.recoverStorageManager.append("group", msg1);
        this.recoverStorageManager.append("group", msg1);
        this.recoverStorageManager.append("group", msg1);
        this.recoverStorageManager.append("group", msg1);
        System.out.println("---" + this.recoverStorageManager.getOrCreateStore("test", "group").size());
    }

}