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
package com.taobao.metamorphosis;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç07:57:46
 */

public class ServerStartupTest {
    private MetaMorphosisBroker metaMorphosisBroker;
    private BrokerCommandProcessor brokerCommandProcessor;
    private BrokerZooKeeper brokerZooKeeper;
    private IMocksControl mocksControl;


    @Before
    public void setup() {
        this.mocksControl = EasyMock.createControl();
        this.metaMorphosisBroker = this.mocksControl.createMock(MetaMorphosisBroker.class);
        this.brokerCommandProcessor = this.mocksControl.createMock(BrokerCommandProcessor.class);
        this.brokerZooKeeper = this.mocksControl.createMock(BrokerZooKeeper.class);

    }


    @Test
    public void testGetPluginsInfo() {
        final String[] args =
                StringUtils.split(
                    "./meta-server-start.sh -f ../conf/server.properties -FjettyBroker jettyBroker.properties", " ");
        final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());
        final Map<String, Properties> pluginsInfo = ServerStartup.getPluginsInfo(line);
        Assert.assertTrue(pluginsInfo.get("jettyBroker").getProperty("serverPort").equals("8080"));

        // EasyMock.expect(this.metaMorphosisBroker.).andReturn(this.brokerCommandProcessor)
        // .anyTimes();
        // EasyMock.expect(this.metaMorphosisBroker.getMetaConfig()).andReturn(new
        // MetaConfig());
        EasyMock.expect(this.metaMorphosisBroker.getBrokerProcessor()).andReturn(null);
        this.mocksControl.replay();

        final BrokerPlugins brokerPlugins = new BrokerPlugins(pluginsInfo, this.metaMorphosisBroker);
        brokerPlugins.init(this.metaMorphosisBroker, null);
        this.mocksControl.verify();
        Assert.assertTrue(brokerPlugins.getPluginsInfo().size() == 1);
        Assert.assertTrue(brokerPlugins.getPluginsInfo().containsKey("jettyBroker"));
    }


    @Test
    public void testGetPluginsInfo_metaslave() {
        final String[] args =
                StringUtils.split(
                    "./meta-server-start.sh -f ../conf/server.properties -Fmetaslave async_slave.properties", " ");
        final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());
        final Map<String, Properties> pluginsInfo = ServerStartup.getPluginsInfo(line);
        Assert.assertTrue(pluginsInfo.get("metaslave").getProperty("slaveId").equals("1"));
        Assert.assertTrue(pluginsInfo.get("metaslave").getProperty("slaveGroup").equals("meta-slave-group"));
        Assert.assertTrue(pluginsInfo.get("metaslave").getProperty("slaveMaxDelayInMills").equals("500"));

        EasyMock.expect(this.metaMorphosisBroker.getBrokerZooKeeper()).andReturn(brokerZooKeeper).anyTimes();
        // this.brokerZooKeeper.resetBrokerIdPath();
        // EasyMock.expect(this.metaMorphosisBroker.getStoreManager()).andReturn(null);
        // EasyMock.expect(metaMorphosisBroker.getIdWorker()).andReturn(null);
        // EasyMock.expect(this.metaMorphosisBroker.getStatsManager()).andReturn(null);
        // EasyMock.expect(this.metaMorphosisBroker.getBrokerProcessor()).andReturn(this.brokerCommandProcessor);
        final MetaConfig metaConfig = new MetaConfig();
        // metaConfig.setSlaveId(1);
        EasyMock.expect(this.metaMorphosisBroker.getMetaConfig()).andReturn(metaConfig).anyTimes();
        this.mocksControl.replay();

        final BrokerPlugins brokerPlugins = new BrokerPlugins(pluginsInfo, this.metaMorphosisBroker);
        // brokerPlugins.init(this.metaMorphosisBroker, null);
        this.mocksControl.verify();

        Assert.assertTrue(brokerPlugins.getPluginsInfo().size() == 1);
        Assert.assertTrue(brokerPlugins.getPluginsInfo().containsKey("metaslave"));
    }
}