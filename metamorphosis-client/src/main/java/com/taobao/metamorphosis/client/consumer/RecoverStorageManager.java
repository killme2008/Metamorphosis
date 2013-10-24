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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.store.Store;
import com.taobao.gecko.core.util.ConcurrentHashSet;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.storage.MessageStore;
import com.taobao.metamorphosis.exception.GetRecoverStorageErrorException;
import com.taobao.metamorphosis.utils.NamedThreadFactory;


/**
 * Recover存储管理器
 * 
 * @author boyan
 * @Date 2011-5-12
 * 
 */
public class RecoverStorageManager extends AbstractRecoverManager {
    private static final String SPLIT = "@";
    private final ConcurrentHashMap<String/* topic */, FutureTask<Store>> topicStoreMap =
            new ConcurrentHashMap<String, FutureTask<Store>>();
    static final Log log = LogFactory.getLog(RecoverStorageManager.class);
    public static final String META_RECOVER_STORE_PATH = System.getProperty("meta.recover.path",
        System.getProperty("user.home") + File.separator + ".meta_recover");

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final SubscribeInfoManager subscribeInfoManager;

    private final ThreadPoolExecutor threadPoolExecutor;

    private final AtomicLong count = new AtomicLong(0);

    private boolean started;


    public RecoverStorageManager(final MetaClientConfig metaClientConfig,
            final SubscribeInfoManager subscribeInfoManager) {
        super();

        // 强制使用caller run策略
        this.threadPoolExecutor =
                new ThreadPoolExecutor(metaClientConfig.getRecoverThreadCount(),
                    metaClientConfig.getRecoverThreadCount(), 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                            100), new NamedThreadFactory("Recover-thread"), new ThreadPoolExecutor.CallerRunsPolicy());
        // this.startRecover(metaClientConfig);
        this.makeDataDir();
        this.subscribeInfoManager = subscribeInfoManager;
        this.loadStores();
    }


    @Override
    public synchronized boolean isStarted() {
        return this.started;
    }


    @Override
    public synchronized void start(final MetaClientConfig metaClientConfig) {
        if (this.started) {
            return;
        }
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                RecoverStorageManager.this.recover();

            }
        }, 5000, metaClientConfig.getRecoverMessageIntervalInMills(), TimeUnit.MILLISECONDS);
        this.started = true;
    }


    private void loadStores() {
        final File dataPath = new File(META_RECOVER_STORE_PATH);
        final File[] files = dataPath.listFiles();
        for (final File subFile : files) {
            if (subFile.isDirectory()) {

                final String name = subFile.getName();
                final String[] tmps = name.split(SPLIT);
                if (tmps.length != 2) {
                    continue;
                }
                log.info("加载recover storage " + name + " ...");
                this.getOrCreateStore(tmps[0], tmps[1]);
            }
        }
    }


    private void makeDataDir() {
        final File file = new File(META_RECOVER_STORE_PATH);
        if (!file.exists()) {
            file.mkdir();
        }
    }


    private void recover() {
        for (final Map.Entry<String, FutureTask<Store>> entry : this.topicStoreMap.entrySet()) {
            this.threadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    final String name = entry.getKey();
                    final String[] tmps = name.split(SPLIT);
                    final String topic = tmps[0];
                    final String group = tmps[1];
                    final FutureTask<Store> task = entry.getValue();
                    final Store store = RecoverStorageManager.this.getStore(topic, task);
                    try {
                        final MessageListener listener =
                                RecoverStorageManager.this.subscribeInfoManager.getMessageListener(topic, group);
                        final Iterator<byte[]> it = store.iterator();
                        int count = 0;
                        while (it.hasNext()) {
                            final byte[] key = it.next();
                            final Message msg =
                                    (Message) RecoverStorageManager.this.deserializer.decodeObject(store.get(key));
                            if (msg != null) {
                                RecoverStorageManager.this.receiveMessage(store, key, msg, listener);
                            }
                            count++;
                        }
                        log.info("Recover topic=" + topic + "恢复消息" + count + "条");
                    }
                    catch (InterruptedException e) {
                        // receive messages is interrupted,we have to interrupt
                        // all executors threads
                        if (Thread.currentThread().isInterrupted()) {
                            for (Thread thread : RecoverStorageManager.this.executorThreads) {
                                thread.interrupt();
                            }
                        }
                    }
                    catch (final Exception e) {
                        log.error("Recover message failed,topic=" + topic, e);
                    }
                }
            });

        }
    }

    boolean wasFirst = true;

    private final ConcurrentHashSet<Thread> executorThreads = new ConcurrentHashSet<Thread>();


    private void receiveMessage(final Store store, final byte[] key, final Message msg,
            final MessageListener messageListener) throws InterruptedException {
        if (messageListener == null) {
            // throw new
            // IllegalStateException("messageListener为null,可能是消费者还未创建,如果是第一次报错，后续没报错了请忽略");
            if (this.wasFirst) {
                log.warn("messageListener为null,可能是消费者还未创建");
                this.wasFirst = false;
            }
            return;
        }
        if (messageListener.getExecutor() != null) {
            try {
                messageListener.getExecutor().execute(new Runnable() {

                    @Override
                    public void run() {
                        Thread currentThread = Thread.currentThread();
                        RecoverStorageManager.this.executorThreads.add(currentThread);
                        try {
                            try {
                                RecoverStorageManager.this.notifyListener(store, key, msg, messageListener);
                            }
                            catch (InterruptedException e) {
                                // Receive messages is interrrupted.
                            }
                        }
                        finally {
                            RecoverStorageManager.this.executorThreads.remove(currentThread);
                        }
                    }

                });
            }
            catch (final RejectedExecutionException e) {

            }
        }
        else {
            this.notifyListener(store, key, msg, messageListener);
        }
    }


    private void notifyListener(final Store store, final byte[] key, final Message msg,
            final MessageListener messageListener) throws InterruptedException {
        messageListener.recieveMessages(msg);
        try {
            store.remove(key);
        }
        catch (final IOException e) {
            log.error("Remove message failed", e);
        }
    }


    @Override
    public void shutdown() {
        for (final Map.Entry<String, FutureTask<Store>> entry : this.topicStoreMap.entrySet()) {
            final String name = entry.getKey();
            final String[] tmps = name.split(SPLIT);
            final String topic = tmps[0];
            final FutureTask<Store> task = entry.getValue();
            final Store store = this.getStore(topic, task);
            try {
                store.close();
            }
            catch (final IOException e) {
                // ignore
            }
        }
        this.threadPoolExecutor.shutdown();
        for (Thread thread : this.executorThreads) {
            thread.interrupt();
        }
        this.scheduledExecutorService.shutdown();
    }


    @Override
    public void append(final String group, final Message message) throws IOException {
        final Store store = this.getOrCreateStore(message.getTopic(), group);
        long key = message.getId();
        IOException error = null;
        for (int i = 0; i < 5; i++) {
            try {
                final ByteBuffer buf = ByteBuffer.allocate(16);
                buf.putLong(key);
                store.add(buf.array(), this.serializer.encodeObject(message));
                return;
            }
            catch (final IOException e) {
                final String msg = e.getMessage();
                // key重复
                if (msg.contains("重复")) {
                    error = e;
                    log.warn("写入recover store出错,key=" + key + "," + e.getMessage() + ",retry...");
                    key += this.count.incrementAndGet();
                }
                else {
                    throw e;
                }
            }
        }

        if (error != null) {
            throw error;
        }

    }


    public Store getOrCreateStore(final String topic, final String group) {
        final String name = this.generateKey(topic, group);
        FutureTask<Store> task = this.topicStoreMap.get(name);
        if (task != null) {
            return this.getStore(topic, task);
        }
        else {
            task = new FutureTask<Store>(new Callable<Store>() {

                @Override
                public Store call() throws Exception {
                    final File file = new File(META_RECOVER_STORE_PATH + File.separator + name);
                    if (!file.exists()) {
                        file.mkdir();
                    }
                    return new MessageStore(META_RECOVER_STORE_PATH + File.separator + name, name);
                }

            });
            FutureTask<Store> existsTask = this.topicStoreMap.putIfAbsent(name, task);
            if (existsTask == null) {
                task.run();
                existsTask = task;
            }
            return this.getStore(name, existsTask);
        }

    }


    private String generateKey(final String topic, final String group) {
        return topic + SPLIT + group;
    }


    private Store getStore(final String name, final FutureTask<Store> task) {
        try {
            return task.get();
        }
        catch (final Throwable t) {
            log.error("获取name=" + name + "对应的store失败", t);
            throw new GetRecoverStorageErrorException("获取topic=" + name + "对应的store失败", t);
        }

    }
}