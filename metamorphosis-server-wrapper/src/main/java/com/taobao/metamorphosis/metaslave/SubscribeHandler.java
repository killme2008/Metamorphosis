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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.MetaMBeanServer;


/**
 * 负责从master接收消息并存储到slaver
 * 
 * @author 无花
 * @since 2011-6-24 下午06:03:29
 */

public class SubscribeHandler implements SubscribeHandlerMBean {
    private final static Log log = LogFactory.getLog(SubscribeHandler.class);

    private final MetaMorphosisBroker broker;
    private final SlaveZooKeeper slaveZooKeeper;
    private final SlaveMetaMessageSessionFactory sessionFactory;
    private final MessageListener messageListener;
    private MessageConsumer consumer;
    private final SlaveOffsetStorage slaveOffsetStorage;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private String masterServerUrl;


    public SubscribeHandler(final MetaMorphosisBroker broker) throws MetaClientException {
        this.broker = broker;
        this.slaveZooKeeper = new SlaveZooKeeper(this.broker, this);
        final MetaConfig metaConfig = this.broker.getMetaConfig();

        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setDiamondZKDataId(metaConfig.getDiamondZKDataId());
        metaClientConfig.setDiamondZKGroup(metaConfig.getDiamondZKGroup());
        metaClientConfig.setZkConfig(metaConfig.getZkConfig());
        this.sessionFactory = SlaveMetaMessageSessionFactory.create(metaClientConfig, metaConfig.getBrokerId());
        this.slaveOffsetStorage =
                new SlaveOffsetStorage(this.broker, this.slaveZooKeeper, this.sessionFactory.getRemotingClient());
        this.messageListener =
                new MetaSlaveListener(this.broker.getBrokerZooKeeper(), this.broker.getStoreManager(),
                    new SlaveStatsManager(this.broker.getStatsManager()));
        MetaMBeanServer.registMBean(this, null);

    }


    // 发生在slave broker启动之后
    synchronized public void start() {
        if (this.isStarted.get()) {
            log.info("Subscriber has been started");
            return;
        }

        try {
            final Map<String, List<Partition>> partitionsForTopics =
                    this.slaveZooKeeper.getPartitionsForTopicsFromMaster();
            if (partitionsForTopics == null || partitionsForTopics.isEmpty()) {
                throw new SubscribeMasterMessageException("topics in master is empty");
            }
            // 生成消息文件,如果有必要的话
            this.handleMassageFiles(partitionsForTopics);
            this.initMessageConsumer();
            this.subscribeMetaMaster(partitionsForTopics.keySet());
            this.isStarted.set(true);
            log.info("start subscribe message from master success");
        }
        catch (final Throwable e) {
            // 由于master没启动或其他原因导致启动数据复制失败的,记录一下,
            // 并继续监听zk,发现master时启动数据复制
            log.warn("problem occured in start subscribe message from master,maybe master not started or other errers",
                e);
            log.info("waiting for master start next time...");
        }
        this.slaveZooKeeper.start();
    }


    /** 当一个topic下某个patition不存在消息文件时,从master查询offset并根据这个offset创建store和文件 **/
    private void handleMassageFiles(final Map<String, List<Partition>> partitionsForTopics) {
        try {
            this.masterServerUrl = this.slaveZooKeeper.getMasterServerUrl();
            for (final Map.Entry<String, List<Partition>> each : partitionsForTopics.entrySet()) {
                for (final Partition partition : each.getValue()) {
                    final String topic = each.getKey();
                    final MessageStore messageStore =
                            this.broker.getStoreManager().getMessageStore(topic, partition.getPartition());
                    // 本地不存在消息文件
                    final StringBuilder logStr = new StringBuilder();
                    if (messageStore == null) {
                        logStr.append("Local file for topic=").append(topic).append(" is not exist,partition=")
                            .append(partition.getPartition()).append("\n");
                        final long offsetInMaster =
                                this.slaveOffsetStorage.queryOffsetInMaster(this.masterServerUrl, partition, topic);
                        logStr.append("query offset from master,offset=").append(offsetInMaster).append("\n");
                        // 创建一个从指定offset开始的store和文件
                        if (offsetInMaster > 0) {
                            this.broker.getStoreManager().getOrCreateMessageStore(topic, partition.getPartition(),
                                offsetInMaster);
                            logStr.append("create messageStore,offset=").append(offsetInMaster).append("\n");
                        }
                        log.info(logStr.toString());
                    }
                }
            }
        }
        catch (final Throwable e) {
            throw new SubscribeMasterMessageException("errer occur on handleMassageFiles", e);
        }
    }


    public void closeConnectIfNeed() {
        if (StringUtils.isBlank(this.masterServerUrl)) {
            log.warn("can not close connect,master server url is blank");
            return;
        }
        try {
            this.sessionFactory.getRemotingClient().close(this.masterServerUrl, false);
            log.info("Closing " + this.masterServerUrl);
            if (log.isDebugEnabled()) {
                log.debug("connect count="
                        + this.sessionFactory.getRemotingClient().getConnectionCount(this.masterServerUrl));
            }
        }
        catch (final NotifyRemotingException e) {
            log.error("close connect " + this.masterServerUrl + " failed", e);
        }
    }


    public void stop() {
        try {
            if (this.isStarted.compareAndSet(true, false)) {
                log.info("stop subscribe from master");
                this.consumer.shutdown();
                this.consumer = null;
                log.info("stop success");
            }
            else {
                log.info("can not stop,since this not started");
            }
        }
        catch (final MetaClientException e) {
            log.error("stop consumer failed", e);
        }
    }


    public void shutdown() {
        this.stop();
        try {
            this.sessionFactory.shutdown();
        }
        catch (final MetaClientException e) {
            log.error(e);
        }
    }


    private void initMessageConsumer() throws Exception {
        if (this.consumer == null) {
            final ConsumerConfig consumerConfig = new ConsumerConfig(this.broker.getMetaConfig().getSlaveGroup());
            consumerConfig.setMaxDelayFetchTimeInMills(this.broker.getMetaConfig().getSlaveMaxDelayInMills());
            consumerConfig.setMaxFetchRetries(Integer.MAX_VALUE);// 消息处理失败卡在那一条上,不进入recover
            this.consumer = this.sessionFactory.createConsumer(consumerConfig, this.slaveOffsetStorage);
        }
        else {
            log.warn("consumer existed");
        }
    }


    private void subscribeMetaMaster(final Set<String> topics) throws Exception {
        try {
            for (final String topic : topics) {
                this.consumer.subscribe(topic, 1024 * 1024, this.messageListener);
            }
            this.consumer.completeSubscribe();
        }
        catch (final Exception e) {
            log.warn("start subscribe from meta master fail", e);
            throw e;
        }
    }


    // for test
    SlaveZooKeeper getSlaveZooKeeper() {
        return this.slaveZooKeeper;
    }


    // for test
    boolean isStarted() {
        return this.isStarted.get();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.metamorphosis.metaslave.SubscribeHandlerMBean#restart()
     */
    @Override
    public void restart() {
        this.stop();
        this.start();
    }
}