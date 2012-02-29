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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.producer.AsyncMessageProducer.IgnoreMessageProcessor;
import com.taobao.metamorphosis.client.extension.producer.MessageRecoverManager.MessageRecoverer;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 
 * @author 无花
 * @since 2011-10-27 上午11:43:35
 */

class AsyncIgnoreMessageProcessor implements IgnoreMessageProcessor {

    private static final Log log = LogFactory.getLog(AsyncIgnoreMessageProcessor.class);

    private static final String STORAGE_PATH = System.getProperty("meta.async.storage.path",
        System.getProperty("user.home") + File.separator + ".meta_async_storage");

    /**
     * 本地磁盘缓存的消息条数限制
     */
    private final int messageCountLimit = 500000;

    private MessageRecoverManager storageManager;


    AsyncIgnoreMessageProcessor(MetaClientConfig metaClientConfig, MessageRecoverer recoverer) {
        this.storageManager = new LocalMessageStorageManager(metaClientConfig, STORAGE_PATH, recoverer);
    }


    /**
     * 消息存入本地磁盘并定期恢复
     */
    @Override
    public boolean handle(Message message) throws Exception {
        Partition partition = message.getPartition();
        partition = (partition != null ? partition : Partition.RandomPartiton);
        int count = this.storageManager.getMessageCount(message.getTopic(), partition);
        if (count < this.messageCountLimit) {
            this.storageManager.append(message, partition);
            return true;
        }
        else {
            log.info("local storage is full,ignore message");
            return false;
        }
    }


    // for test
    void setStorageManager(MessageRecoverManager storageManager) {
        this.storageManager = storageManager;
    }
}