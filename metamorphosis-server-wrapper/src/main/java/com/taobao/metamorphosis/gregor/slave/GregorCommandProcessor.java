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
package com.taobao.metamorphosis.gregor.slave;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.service.RemotingServer;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.ExecutorsManager;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.IdWorker;


/**
 * Slave的协议处理器
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class GregorCommandProcessor extends BrokerCommandProcessor implements SyncCommandProcessor {

    public static final Log log = LogFactory.getLog(GregorCommandProcessor.class);


    public GregorCommandProcessor() {
        super();
    }


    public GregorCommandProcessor(final MessageStoreManager storeManager, final ExecutorsManager executorsManager,
            final StatsManager statsManager, final RemotingServer remotingServer, final MetaConfig metaConfig,
            final IdWorker idWorker, final BrokerZooKeeper brokerZooKeeper, ConsumerFilterManager consumerFilterManager) {
        super(storeManager, executorsManager, statsManager, remotingServer, metaConfig, idWorker, brokerZooKeeper,
            consumerFilterManager);
    }


    @Override
    public void processSyncCommand(final SyncCommand request, final SessionContext sessionContext, final PutCallback cb) {
        final String partitionString = this.metaConfig.getBrokerId() + "-" + request.getPartition();
        this.statsManager.statsPut(request.getTopic(), partitionString, 1);
        this.statsManager.statsMessageSize(request.getTopic(), request.getData().length);
        try {
            if (this.metaConfig.isClosedPartition(request.getTopic(), request.getPartition())) {
                log.warn("Can not put message to partition " + request.getPartition() + " for topic="
                        + request.getTopic() + ",it was closed");
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(HttpStatus.Forbidden, this.genErrorMessage(request.getTopic(),
                        request.getPartition()) + "Detail:Partition[" + partitionString + "] has been closed", request
                        .getOpaque()));
                }
                return;
            }

            // 使用master发过来的分区
            final int partition = request.getPartition();
            final MessageStore store = this.storeManager.getOrCreateMessageStore(request.getTopic(), partition);
            // 使用master发过来的id
            final long messageId = request.getMsgId();
            store.append(messageId, request,
                new StoreAppendCallback(partition, partitionString, request, messageId, cb));
        }
        catch (final Exception e) {
            this.statsManager.statsPutFailed(request.getTopic(), partitionString, 1);
            log.error("Put message failed", e);
            if (cb != null) {
                cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, this.genErrorMessage(
                    request.getTopic(), request.getPartition())
                    + "Detail:" + e.getMessage(), request.getOpaque()));
            }
        }
    }

}