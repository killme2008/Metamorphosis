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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.taobao.metamorphosis.gregor.master.SamsaMasterBroker;
import com.taobao.metamorphosis.gregor.slave.GregorSlaveBroker;
import com.taobao.metamorphosis.http.MetamorphosisOnJettyBroker;
import com.taobao.metamorphosis.metaslave.MetamorphosisSlaveBroker;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;


/**
 * @author 无花
 * @since 2011-6-9 下午01:36:39
 */

public class BrokerPlugins extends AbstractBrokerPlugin {

    /**
     * 已注册的plugins
     */
    private final Map<String/* plugin name */, BrokerPlugin> plugins = new HashMap<String, BrokerPlugin>();

    /**
     * 需要启动的plugins
     */
    private final Map<String/* plugin name */, Properties> pluginsInfo = new HashMap<String, Properties>();

    private final AtomicBoolean isInited = new AtomicBoolean(false);


    public BrokerPlugins(final Map<String, Properties> pluginsInfo, final MetaMorphosisBroker broker) {
        // this.register(TimetunnelBroker.class);
        // this.register(SlaveBroker.class);
        this.register(MetamorphosisSlaveBroker.class);
        // this.register(NotifyAdaperBroker.class);
        this.register(MetamorphosisOnJettyBroker.class);
        this.register(SamsaMasterBroker.class);
        this.register(GregorSlaveBroker.class);
        this.broker = broker;

        if (pluginsInfo != null) {
            this.pluginsInfo.putAll(pluginsInfo);
        }

        this.checkPluginsInfo(this.plugins, this.pluginsInfo);
    }


    private void checkPluginsInfo(final Map<String, BrokerPlugin> plugins, final Map<String, Properties> pluginsInfo) {
        if (pluginsInfo == null || pluginsInfo.isEmpty()) {
            log.info("no broker plugin");
            return;
        }

        // 作为异步复制Slave启动时特殊处理，不启动其他plugin
        if (pluginsInfo.containsKey("metaslave")) {
            log.info("start as meta slaver,unstart other plugins");
            final Properties slaveProperties = pluginsInfo.get("metaslave");
            pluginsInfo.clear();
            pluginsInfo.put("metaslave", slaveProperties);
        }

        for (final String name : pluginsInfo.keySet()) {
            log.info("cmd line require start plugin:" + name);
            if (plugins.get(name) == null) {
                throw new MetamorphosisServerStartupException("unknown broker plugin:" + name);
            }
        }
    }


    @Override
    public void init(final MetaMorphosisBroker broker, final Properties props) {
        if (this.isInited.compareAndSet(false, true)) {
            new InnerPluginsRunner() {
                @Override
                protected void doExecute(final BrokerPlugin plugin) {
                    log.info("Start inited broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName() + "]");
                    plugin.init(broker, BrokerPlugins.this.pluginsInfo.get(plugin.name()));
                    log.info("Inited broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName() + "]");
                }
            }.execute();
        }
    }


    void register(final Class<? extends BrokerPlugin> pluginClass) {
        try {
            final BrokerPlugin plugin = pluginClass.getConstructor(new Class[0]).newInstance();
            this.plugins.put(plugin.name(), plugin);
        }
        catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Register broker plugin failed", e);
        }
    }


    @Override
    public String name() {
        return null;
    }


    @Override
    public void start() {
        if (!this.isInited.get()) {
            log.warn("Not inited yet");
            return;
        }

        new InnerPluginsRunner() {
            @Override
            protected void doExecute(final BrokerPlugin plugin) {
                plugin.start();
                log.info("Started broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName() + "]");
            }
        }.execute();
    }


    @Override
    public void stop() {
        new InnerPluginsRunner() {
            @Override
            protected void doExecute(final BrokerPlugin plugin) {
                plugin.stop();
                BrokerPlugins.log.info("stoped broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName()
                        + "]");
            }
        }.execute();
    }

    private abstract class InnerPluginsRunner {

        public void execute() {
            for (final BrokerPlugin plugin : BrokerPlugins.this.plugins.values()) {
                if (BrokerPlugins.this.pluginsInfo.containsKey(plugin.name())) {
                    this.doExecute(plugin);
                }
                else {
                    BrokerPlugins.log.info("unstarted plugin:" + plugin.name());
                }
            }
        }


        protected abstract void doExecute(BrokerPlugin plugin);
    }


    // for test
    Map<String, Properties> getPluginsInfo() {
        return this.pluginsInfo;
    }

}