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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.taobao.common.store.Store;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.storage.MessageStore;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 针对顺序消息做了特殊处理的LocalMessageStorageManager
 * 
 * @author 无花
 * @since 2011-10-27 下午3:21:32
 */

public class OrderedLocalMessageStorageManager extends LocalMessageStorageManager {

    public OrderedLocalMessageStorageManager(MetaClientConfig metaClientConfig) {
        super(metaClientConfig);
    }


    @Override
    public void recover() {
        Set<String> names = this.topicStoreMap.keySet();
        if (names == null || names.size() == 0) {
            log.info("SendRecover没有需要恢复的消息");
            return;
        }

        if (this.messageRecoverer != null) {
            for (String name : names) {
                String[] tmps = name.split(SPLIT);
                String topic = tmps[0];
                Partition partition = new Partition(tmps[1]);
                if (!partition.equals(Partition.RandomPartiton) && this.getMessageCount(topic, partition) > 0) {
                    // RandomPartiton会在里面recover
                    this.recover(topic, partition, this.messageRecoverer);
                }
            }
        }
        else {
            log.warn("messageRecoverer还未设置");
        }
    }


    /**
     * 触发恢复一个主题一个分区的消息,可多次调用(保证对某主题的恢复任务最多只有一个在运行)
     * 
     * @param topic
     * @param partition
     * @param recoverer
     *            恢复出来的消息的处理器
     * @return 是否真正提交了恢复任务
     * */
    @Override
    public boolean recover(final String topic, final Partition partition, final MessageRecoverer recoverer) {

        final String name = this.generateKey(topic, partition);
        FutureTask<Boolean> recoverTask = new FutureTask<Boolean>(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                try {
                    int count = 0;
                    Store randomPartitonStore =
                            OrderedLocalMessageStorageManager.this.getOrCreateStore(topic, Partition.RandomPartiton);

                    // 不管恢复哪个分区都优先恢复一下未知分区的数据(原则是,这个分区数据的产生时间总是先于具体分区)
                    // 不能让同一个topic的未知分区并行恢复，否则会导致数据重复发送
                    if (randomPartitonStore.size() > 0) {
                        // 有数据时才对randomPartitonStore同步竞争
                        synchronized (randomPartitonStore) {
                            // 双重检测
                            if (randomPartitonStore.size() > 0) {
                                count = this.innerRecover(randomPartitonStore, recoverer);
                                log.info("SendRecover topic=" + topic + "@-1--1恢复消息" + count + "条");
                            }
                        }
                    }

                    Store store = OrderedLocalMessageStorageManager.this.getOrCreateStore(topic, partition);

                    count = this.innerRecover(store, recoverer);
                    log.info("SendRecover topic=" + name + "恢复消息" + count + "条");
                }
                catch (Throwable e) {
                    log.error("SendRecover发送消息恢复失败,topic=" + name, e);
                }
                finally {
                    log.info("SendRecover执行完毕移除发送恢复任务,topic=" + name);
                    OrderedLocalMessageStorageManager.this.topicRecoverTaskMap.remove(name);
                }
                return true;
            }


            private int innerRecover(Store store, final MessageRecoverer recoverer) throws IOException, Exception {
                Iterator<byte[]> it = store.iterator();
                int count = 0;
                while (it.hasNext()) {
                    byte[] key = it.next();
                    Message msg =
                            (Message) OrderedLocalMessageStorageManager.this.deserializer.decodeObject(store.get(key));
                    recoverer.handle(msg);
                    try {
                        store.remove(key);
                        count++;
                    }
                    catch (IOException e) {
                        log.error("SendRecover remove message failed", e);
                    }
                }
                return count;
            }
        });

        FutureTask<Boolean> ret = this.topicRecoverTaskMap.putIfAbsent(name, recoverTask);
        if (ret == null) {
            this.threadPoolExecutor.submit(recoverTask);
            return true;
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("SendRecover发送恢复任务正在运行,不需要重新启动,topic=" + topic);
            }
            return false;
        }

    }


    @Override
    protected Store newStore(final String name) throws IOException {
        return new MessageStore(META_LOCALMESSAGE_PATH + File.separator + name, name);
    }

}