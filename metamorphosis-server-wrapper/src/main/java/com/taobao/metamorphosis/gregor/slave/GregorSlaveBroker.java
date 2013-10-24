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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.gregor.Constants;
import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.NamedThreadFactory;


/**
 * Slave broker，可怜的格利高尔
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class GregorSlaveBroker extends AbstractBrokerPlugin {
    OrderedThreadPoolExecutor orderedPutExecutor;


    @Override
    public void start() {

    }


    @Override
    public void stop() {
        if (this.orderedPutExecutor != null) {
            this.orderedPutExecutor.shutdown();
        }

    }


    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        final MetaConfig metaConfig = metaMorphosisBroker.getMetaConfig();
        // Added slave status topic for master to check it.
        TopicConfig topicConfig = new TopicConfig(Constants.TEST_SLAVE_TOPIC, metaConfig);
        topicConfig.setNumPartitions(1);
        metaConfig.addTopic(Constants.TEST_SLAVE_TOPIC, topicConfig);
        // slave不注册到zk,强制
        metaMorphosisBroker.getBrokerZooKeeper().getZkConfig().zkEnable = false;
        this.orderedPutExecutor =
                new OrderedThreadPoolExecutor(metaConfig.getPutProcessThreadCount(),
                    metaConfig.getPutProcessThreadCount(), 60, TimeUnit.SECONDS, new NamedThreadFactory("putProcessor"));
        final GregorCommandProcessor processor =
                new GregorCommandProcessor(metaMorphosisBroker.getStoreManager(),
                    metaMorphosisBroker.getExecutorsManager(), metaMorphosisBroker.getStatsManager(),
                    metaMorphosisBroker.getRemotingServer(), metaMorphosisBroker.getMetaConfig(),
                    metaMorphosisBroker.getIdWorker(), metaMorphosisBroker.getBrokerZooKeeper(),
                    metaMorphosisBroker.getConsumerFilterManager());
        // 强制设置processor
        metaMorphosisBroker.setBrokerProcessor(processor);
        final SyncProcessor syncProcessor = new SyncProcessor(processor, this.orderedPutExecutor);
        // 注册网络处理器
        metaMorphosisBroker.getRemotingServer().registerProcessor(SyncCommand.class, syncProcessor);
    }


    @Override
    public String name() {
        return "gregor";
    }

}