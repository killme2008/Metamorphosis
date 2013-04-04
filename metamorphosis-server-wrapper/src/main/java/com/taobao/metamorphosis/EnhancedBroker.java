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
package com.taobao.metamorphosis;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.killme2008.metamorphosis.dashboard.Server;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * @author 无花
 * @since 2011-6-9 下午01:27:12
 */

public class EnhancedBroker {

    static final Log log = LogFactory.getLog(EnhancedBroker.class);

    private final MetaMorphosisBroker broker;

    private final BrokerPlugins brokerPlugins;

    private org.eclipse.jetty.server.Server jettyServer;


    public void start() {
        // 先启动meta,然后启动Plugins
        this.broker.start();
        this.brokerPlugins.start();
        Server dashboradHttpServer = new Server();
        this.jettyServer = dashboradHttpServer.start(this.broker);
    }


    public void stop() {
        try {
            this.jettyServer.stop();
        }
        catch (Exception e) {
            log.error("Stop dashboard http server failed", e);
        }
        this.brokerPlugins.stop();
        this.broker.stop();
    }


    public EnhancedBroker(final MetaConfig metaConfig, final Map<String/*
     * plugin
     * name
     */, Properties> pluginsInfo) {
        this.broker = new MetaMorphosisBroker(metaConfig);
        this.brokerPlugins = new BrokerPlugins(pluginsInfo, this.broker);
        this.brokerPlugins.init(this.broker, null);
    }


    public MetaMorphosisBroker getBroker() {
        return this.broker;
    }

}