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
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.utils.Utils;


/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç03:45:45
 */

public class StartupHelp {
    static final Log log = LogFactory.getLog(StartupHelp.class);


    public static CommandLine parseCmdLine(final String[] args, final CommandLineParser parser) {
        return parseCmdLine(args, options(), parser);
    }


    public static CommandLine parseCmdLine(final String[] args, final Options options, final CommandLineParser parser) {
        final HelpFormatter hf = new HelpFormatter();
        try {
            return parser.parse(options, args);
        }
        catch (final ParseException e) {
            hf.printHelp("ServerStartup", options, true);
            log.error("Parse command line failed", e);
            throw new MetamorphosisServerStartupException("Parse command line failed", e);
        }
    }


    public static Options options() {
        final Options options = new Options();
        final Option brokerFile = new Option("f", true, "Broker configuration file path");
        final Option localMode = new Option("l", false, "Broker configuration file path");
        localMode.setRequired(false);
        brokerFile.setRequired(true);
        options.addOption(brokerFile);
        options.addOption(localMode);

        final Option pluginParams =
                OptionBuilder.withArgName("pluginname=configfile").hasArgs(2).withValueSeparator()
                .withDescription("use value for given param").create("F");
        options.addOption(pluginParams);

        return options;
    }


    public static Properties getProps(final String path) {
        try {
            return Utils.getResourceAsProperties(path, "GBK");
        }
        catch (final IOException e) {
            throw new MetamorphosisServerStartupException("Parse configuration failed,path=" + path, e);
        }
    }
}