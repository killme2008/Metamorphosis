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
package com.taobao.metamorphosis.server;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * Metamorphosis服务器启动器
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class MetamorphosisStartup {
    static final Log log = LogFactory.getLog(MetamorphosisStartup.class);


    public static void main(final String[] args) {
        final String configFilePath = getConfigFilePath(args);
        final MetaConfig metaConfig = getMetaConfig(configFilePath);
        final MetaMorphosisBroker server = new MetaMorphosisBroker(metaConfig);
        server.start();

    }


    static MetaConfig getMetaConfig(final String configFilePath) {
        final MetaConfig metaConfig = new MetaConfig();
        metaConfig.loadFromFile(configFilePath);
        metaConfig.verify();
        log.warn("服务器配置为：" + metaConfig);
        System.out.println("准备启动服务器，配置为：" + metaConfig);
        return metaConfig;
    }


    static String getConfigFilePath(final String[] args) throws MetamorphosisServerStartupException {
        final Options options = new Options();
        final Option file = new Option("f", true, "Configuration file path");
        options.addOption(file);
        final CommandLineParser parser = new PosixParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        }
        catch (final ParseException e) {
            throw new MetamorphosisServerStartupException("Parse command line failed", e);
        }
        String configFilePath = null;
        if (line.hasOption("f")) {
            configFilePath = line.getOptionValue("f");
        }
        else {
            System.err.println("Please tell me the configuration file path by -f option");
            System.exit(1);
        }
        if (StringUtils.isBlank(configFilePath)) {
            throw new MetamorphosisServerStartupException("Blank file path");
        }
        return configFilePath;
    }
}