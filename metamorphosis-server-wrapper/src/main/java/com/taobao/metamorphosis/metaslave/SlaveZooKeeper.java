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

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.utils.MetaZookeeper;


/**
 * 负责跟zk交互,并监控master在zk上的注册
 * 
 * @author 无花,dennis
 * @since 2011-6-24 下午05:46:36
 */

class SlaveZooKeeper {

    private static final Log log = LogFactory.getLog(SlaveZooKeeper.class);

    private final MetaMorphosisBroker broker;
    private final SubscribeHandler subscribeHandler;
    private final MasterBrokerIdListener masterBrokerIdListener;
    private final String masterBrokerIdsPath;
    private final String masterConfigFileChecksumPath;


    public SlaveZooKeeper(final MetaMorphosisBroker broker, final SubscribeHandler subscribeHandler) {
        this.broker = broker;
        this.subscribeHandler = subscribeHandler;
        int brokerId = this.broker.getMetaConfig().getBrokerId();
        this.masterBrokerIdsPath = this.getMetaZookeeper().brokerIdsPathOf(brokerId, -1);
        this.masterConfigFileChecksumPath = this.getMetaZookeeper().masterConfigChecksum(brokerId);
        this.masterBrokerIdListener = new MasterBrokerIdListener();
    }


    public void start() {
        // 订阅zk信息变化
        this.getZkClient().subscribeDataChanges(this.masterBrokerIdsPath, this.masterBrokerIdListener);
        this.getZkClient().subscribeDataChanges(this.masterConfigFileChecksumPath, this.masterBrokerIdListener);
    }


    public String getMasterServerUrl() {
        final Broker masterBroker =
                this.getMetaZookeeper().getMasterBrokerById(this.broker.getMetaConfig().getBrokerId());
        return masterBroker != null ? masterBroker.getZKString() : null;
    }


    public Map<String, List<Partition>> getPartitionsForTopicsFromMaster() {
        return this.getMetaZookeeper().getPartitionsForSubTopicsFromMaster(this.getMasterTopics(),
            this.broker.getMetaConfig().getBrokerId());
    }


    private Set<String> getMasterTopics() {
        return this.getMetaZookeeper().getTopicsByBrokerIdFromMaster(this.broker.getMetaConfig().getBrokerId());
    }


    private ZkClient getZkClient() {
        return this.broker.getBrokerZooKeeper().getZkClient();
    }


    private MetaZookeeper getMetaZookeeper() {
        return this.broker.getBrokerZooKeeper().getMetaZookeeper();
    }

    private final class MasterBrokerIdListener implements IZkDataListener {

        @Override
        public synchronized void handleDataChange(final String dataPath, final Object data) throws Exception {
            int zkSyncTimeMs;
            try {
                zkSyncTimeMs = SlaveZooKeeper.this.broker.getMetaConfig().getZkConfig().zkSyncTimeMs;
            }
            catch (final Exception e) {
                zkSyncTimeMs = 5000;
                // ignore
            }
            // 等待zk数据同步完毕再启动订阅
            Thread.sleep(zkSyncTimeMs);
            if (dataPath.equals(SlaveZooKeeper.this.masterBrokerIdsPath)) {
                // 用于slave先启动，master后启动时
                log.info("data changed in zk,path=" + dataPath);
                SlaveZooKeeper.this.subscribeHandler.start();
            }
            else if (dataPath.equals(SlaveZooKeeper.this.masterConfigFileChecksumPath)) {
                log.info("Restart slave...");
                SlaveZooKeeper.this.subscribeHandler.restart();
                log.info("Restart slave successfully.");
            }
            else {
                log.warn("Unknown data path:" + dataPath);
            }
        }


        @Override
        public void handleDataDeleted(final String dataPath) throws Exception {
            log.info("data deleted in zk,path=" + dataPath);
            if (dataPath.equals(SlaveZooKeeper.this.masterBrokerIdsPath)) {
                // 按照RemotingClientWrapper的机制,close的次数要等于connect的次数才能真正关闭掉连接,
                // 要由于在订阅master消息前连接了一次,所以要在这里关闭一次
                // 其他的连接和关闭有负载均衡负责
                SlaveZooKeeper.this.subscribeHandler.closeConnectIfNeed();
            }
        }
    }

}