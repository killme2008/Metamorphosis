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

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.NetworkException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;


/**
 * 从slave消息文件中load offset,没load到时从master查询最小offset
 * 
 * @author 无花
 * @since 2011-6-27 上午10:09:38
 */
public class SlaveOffsetStorage implements OffsetStorage {
    private final static Log log = LogFactory.getLog(SlaveOffsetStorage.class);
    private final MetaMorphosisBroker broker;
    private final RemotingClientWrapper remotingClient;
    private final SlaveZooKeeper slaveZooKeeper;


    public SlaveOffsetStorage(final MetaMorphosisBroker broker, final SlaveZooKeeper slaveZooKeeper,
            final RemotingClientWrapper remotingClient) {
        this.broker = broker;
        this.remotingClient = remotingClient;
        this.slaveZooKeeper = slaveZooKeeper;
    }


    @Override
    public void close() {
        final String masterServerUrl = this.slaveZooKeeper.getMasterServerUrl();
        try {
            if (!StringUtils.isBlank(masterServerUrl)) {
                this.remotingClient.closeWithRef(masterServerUrl, this, true);
            }
        }
        catch (NotifyRemotingException e) {
            // ignore;
        }
    }


    @Override
    public void commitOffset(final String group, final Collection<TopicPartitionRegInfo> infoList) {
        // do nothing
    }


    @Override
    public void initOffset(final String topic, final String group, final Partition partition, final long offset) {
        // do nothing
    }

    private static String offsetFormat = "topic=%s,group=%s,partition=%s,offset=%s";


    @Override
    public TopicPartitionRegInfo load(final String topic, final String group, final Partition partition) {
        // 先从本地查询
        final MessageStoreManager storeManager = this.broker.getStoreManager();
        final MessageStore messageStore = storeManager.getMessageStore(topic, partition.getPartition());
        if (messageStore != null) {
            log.info("load offset from local"
                    + String.format(offsetFormat, topic, group, partition.getPartition(), messageStore.getSizeInBytes()));
            return new TopicPartitionRegInfo(topic, partition, messageStore.getMinOffset()
                + messageStore.getSizeInBytes());
        }
        else {
            final String masterServerUrl = this.slaveZooKeeper.getMasterServerUrl();
            if (StringUtils.isBlank(masterServerUrl)) {
                throw new NullPointerException("masterServerUrl is empty");
            }

            try {
                final long offset = this.queryOffsetInMaster(masterServerUrl, partition, topic);
                log.info("load offset from master,"
                        + String.format(offsetFormat, topic, group, partition.getPartition(), offset));
                return new TopicPartitionRegInfo(topic, partition, offset);
            }
            catch (final Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException("load offset fail,", e);
            }
        }
    }


    /** 正确查到offset时返回,否则均抛出异常 */
    long queryOffsetInMaster(final String masterServerUrl, final Partition partition, final String topic)
            throws NetworkException, InterruptedException {

        // 在客户端订阅消息前可能没连接到服务器
        if (!this.remotingClient.isConnected(masterServerUrl)) {
            log.info("try connect to " + masterServerUrl);
            this.connectServer(masterServerUrl);
        }
        try {
            final BooleanCommand resp =
                    (BooleanCommand) this.remotingClient.invokeToGroup(masterServerUrl, new OffsetCommand(topic,
                        this.broker.getMetaConfig().getSlaveConfig().getSlaveGroup(), partition.getPartition(), 0,
                        OpaqueGenerator.getNextOpaque()));

            final String resultStr = resp.getErrorMsg();

            switch (resp.getCode()) {
            case HttpStatus.Success: {
                return Long.parseLong(resultStr);
            }
            case HttpStatus.NotFound: {
                // 在master还没接收到一条这个topic的消息时,目录还没创建.这里默认offset为0处理
                return 0;
            }
            default:
                throw new RuntimeException("failed to query offset form " + masterServerUrl + ",topic=" + topic
                    + ",partition=" + partition.getPartition() + ",httpCode=" + resp.getCode() + ",errorMessage="
                    + resultStr);
            }

        }
        catch (final InterruptedException e) {
            throw e;
        }
        catch (final Exception e) {
            throw new NetworkException("failed to query offset form " + masterServerUrl + ",topic=" + topic
                + ",partition=" + partition.getPartition(), e);
        }
    }


    private void connectServer(final String serverUrl) throws NetworkException, InterruptedException {
        try {
            this.remotingClient.connectWithRef(serverUrl, this);
            this.remotingClient.awaitReadyInterrupt(serverUrl, 5000);
            // 5秒钟连接不上则认为服务端没起来
            // ,打断查询
        }
        catch (final NotifyRemotingException e) {
            throw new NetworkException("Connect to " + serverUrl + " failed", e);
        }
        catch (final InterruptedException e) {
            throw e;
        }
    }

}