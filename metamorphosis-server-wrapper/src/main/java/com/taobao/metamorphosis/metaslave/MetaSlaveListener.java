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
 *   wuhua <wq163@163.com>
 */
package com.taobao.metamorphosis.metaslave;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.AppendMessageErrorException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.store.AppendCallback;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-23 ÏÂÎç03:59:21
 */

public class MetaSlaveListener implements MessageListener {
    private static final class AppendOp implements AppendCallback {
        final CountDownLatch latch;
        volatile long offset;


        private AppendOp() {
            this.latch = new CountDownLatch(1);
        }


        @Override
        public void appendComplete(final Location location) {
            this.offset = location.getOffset();
            this.latch.countDown();
        }
    }

    private final static Log log = LogFactory.getLog(MessageListener.class);
    private final MessageStoreManager storeManager;
    private final BrokerZooKeeper brokerZooKeeper;
    private final SlaveStatsManager statsManager;


    public MetaSlaveListener(final BrokerZooKeeper brokerZooKeeper, final MessageStoreManager storeManager,
            final SlaveStatsManager slaveStatsManager) {
        super();
        this.brokerZooKeeper = brokerZooKeeper;
        this.storeManager = storeManager;
        this.statsManager = slaveStatsManager;
    }


    @Override
    public Executor getExecutor() {
        return null;
    }

    static final long APPEND_TIMEOUT = 10000L;


    @Override
    public void recieveMessages(final Message message) throws InterruptedException {

        this.statsManager.statsSlavePut(message.getTopic(), message.getPartition().toString(), 1);
        this.statsManager.statsMessageSize(message.getTopic(), message.getData().length);
        final int partition = message.getPartition().getPartition();
        MessageStore store;
        try {
            store = this.storeManager.getOrCreateMessageStore(message.getTopic(), partition);
            final long messageId = message.getId();
            this.brokerZooKeeper.registerTopicInZk(message.getTopic(), false);

            final AppendOp cb = new AppendOp();
            store.append(messageId, new PutCommand(message.getTopic(), partition, MessageUtils.encodePayload(message),
                null, MessageAccessor.getFlag(message), 0), cb);
            cb.latch.await(APPEND_TIMEOUT, TimeUnit.MILLISECONDS);
            if (cb.offset < 0) {
                log.error("offset wasless then 0 when append meta slave message");
                throw new AppendMessageErrorException("Append message failed,topic=" + message.getTopic());
            }
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (final Throwable e) {
            this.statsManager.statsSlavePutFailed(message.getTopic(), message.getPartition().toString(), 1);
            log.error("process meta master message fail", e);
            throw new AppendMessageErrorException("Append message failed,topic=" + message.getTopic(), e);
        }

    }
}