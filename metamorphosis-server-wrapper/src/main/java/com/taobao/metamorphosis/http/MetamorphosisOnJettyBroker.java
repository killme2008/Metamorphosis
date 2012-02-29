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
package com.taobao.metamorphosis.http;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.http.processor.MetamorphosisOnJettyProcessor;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;


/**
 * 代表一个基于Jetty支持Http的Metamorphosis Server
 */

public class MetamorphosisOnJettyBroker extends AbstractBrokerPlugin {
    private static final Log logger = LogFactory.getLog(MetamorphosisOnJettyBroker.class);

    private Server server = null;


    public void start() {
        try {
            server.start();
        }
        catch (Exception e) {
            logger.error("MetamorphosisOnJettyBroker start:" + e.getMessage());
        }
    }


    public void stop() {
        try {
            server.stop();
        }
        catch (Exception e) {
            logger.error("MetamorphosisOnJettyBroker stop:" + e.getMessage());
        }
    }


    public void init(MetaMorphosisBroker metaMorphosisBroker, Properties props) {
        Handler handler = new MetamorphosisOnJettyProcessor(metaMorphosisBroker);
        this.props = props;
        server = new Server();
        server.setHandler(handler);

        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(Integer.parseInt(this.props.getProperty("serverPort", "8080")));
        // all use select default configuration without changes.
        // connector.setMaxIdleTime(30000); //convert to setSoTimout of socket
        // connector.setRequestHeaderSize(8192);

        server.setConnectors(new Connector[] { connector });
    }


    public String name() {
        return "jettyBroker";
    }

}