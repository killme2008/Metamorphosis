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

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;


/**
 * 代表一个向meta master同步消息数据的slaver
 * 
 * @author 无花
 * @since 2011-6-23 下午01:54:11
 */

public class MetamorphosisSlaveBroker extends AbstractBrokerPlugin {

    private SubscribeHandler subscribeHandler;


    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        this.broker = metaMorphosisBroker;
        this.props = props;

        this.putSlaveProperties(this.broker, this.props);

        if (!this.broker.getMetaConfig().isSlave()) {
            throw new SubscribeMasterMessageException("Could not start as a slave broker");
        }

        try {
            this.subscribeHandler = new SubscribeHandler(this.broker);
        }
        catch (final MetaClientException e) {
            throw new SubscribeMasterMessageException("Create subscribeHandler failed", e);
        }
    }


    private void putSlaveProperties(final MetaMorphosisBroker broker, final Properties props) {
        broker.getMetaConfig().setSlaveId(Integer.parseInt(props.getProperty("slaveId")));
        if (StringUtils.isNotBlank(props.getProperty("slaveGroup"))) {
            broker.getMetaConfig().setSlaveGroup(props.getProperty("slaveGroup"));
        }
        if (StringUtils.isNotBlank(props.getProperty("slaveMaxDelayInMills"))) {
            broker.getMetaConfig().setSlaveMaxDelayInMills(Integer.parseInt(props.getProperty("slaveMaxDelayInMills")));
        }

        // 重新设置BrokerIdPath，以便注册到slave的路径
        broker.getBrokerZooKeeper().resetBrokerIdPath();
    }


    @Override
    public String name() {
        return "metaslave";
    }


    @Override
    public void start() {
        this.subscribeHandler.start();
    }


    @Override
    public void stop() {
        this.subscribeHandler.shutdown();
    }
}