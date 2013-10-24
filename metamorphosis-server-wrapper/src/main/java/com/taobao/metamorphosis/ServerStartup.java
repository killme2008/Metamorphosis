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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.assembly.EmbedZookeeperServer;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * @author 无花,dennis<killme2008@gmail.com>
 * @since 2011-6-9 下午03:28:28
 */

public class ServerStartup {
    static final Log log = LogFactory.getLog(ServerStartup.class);


    public static void main(final String[] args) throws IOException, InterruptedException {
        final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());
        final Map<String, Properties> pluginsInfo = getPluginsInfo(line);
        tryStartLocalZookeeper(line);
        MetaConfig metaConfig = getMetaConfig(line);
        final EnhancedBroker broker = new EnhancedBroker(metaConfig, pluginsInfo);
        broker.start();
    }


    private static void tryStartLocalZookeeper(final CommandLine line) throws IOException, InterruptedException {
        final String optionLocal = "l";
        // Startup a local zookeeper server
        if (line.hasOption(optionLocal)) {
            log.warn("Starting metamorphosis broker in local mode,starting a local zookeeper server...(please don't use this mode in your production,you should create a zookeeper cluster instead).");
            EmbedZookeeperServer.getInstance().start();
        }
    }


    private static MetaConfig getMetaConfig(final CommandLine line) {
        final String optionStr = "f";

        String configFilePath = null;
        if (line.hasOption(optionStr)) {
            configFilePath = line.getOptionValue(optionStr);
            if (StringUtils.isBlank(configFilePath)) {
                throw new MetamorphosisServerStartupException("Blank file path");
            }

        }
        else {
            System.err.println("Please tell me the broker configuration file path by -f option");
            System.exit(1);
        }

        final MetaConfig metaConfig = new MetaConfig();
        metaConfig.loadFromFile(configFilePath);
        metaConfig.verify();
        log.warn("服务器配置为：" + metaConfig);
        System.out.println("准备启动服务器，配置为：" + metaConfig);
        return metaConfig;
    }


    static Map<String, Properties> getPluginsInfo(final CommandLine line) {
        final Properties properties = line.getOptionProperties("F");
        final Map<String, Properties> pluginsInfo = new HashMap<String, Properties>();
        if (properties != null) {
            for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
                pluginsInfo.put(String.valueOf(entry.getKey()), StartupHelp.getProps(String.valueOf(entry.getValue())));
            }
        }
        return pluginsInfo;

    }

}