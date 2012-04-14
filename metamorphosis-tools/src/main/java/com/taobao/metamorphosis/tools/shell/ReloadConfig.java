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
package com.taobao.metamorphosis.tools.shell;

import java.io.PrintStream;
import java.io.PrintWriter;

import javax.management.ObjectInstance;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;
import com.taobao.metamorphosis.tools.utils.JMXClient;


/**
 * <pre>
 * 触发重载配置文件. 
 * usage:
 *      ReloadConfig
 *      ReloadConfig -host 10.2.2.3
 *      ReloadConfig -port 9999
 *      ReloadConfig -host 10.2.2.3 -port 9999
 * </pre>
 * 
 * @author 无花
 * @since 2011-7-19 下午06:02:52
 */

public class ReloadConfig extends ShellTool {

    public ReloadConfig(PrintStream out) {
        super(out);
    }


    public ReloadConfig(PrintWriter out) {
        super(out);
    }


    public static void main(String[] args) throws Exception {
        new ReloadConfig(System.out).doMain(args);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        CommandLine commandLine =
                CommandLineUtils.parseCmdLine(args,
                    new Options().addOption("host", true, "host").addOption("port", true, "port"));

        String host = commandLine.getOptionValue("host", "127.0.0.1");
        int port = Integer.parseInt(commandLine.getOptionValue("port", "9123"));

        JMXClient jmxClient = JMXClient.getJMXClient(host, port);

        this.println("connected to " + jmxClient.getAddressAsString());

        ObjectInstance metaConfigInstance = jmxClient.queryMBeanForOne(METACONFIG_NAME);
        if (metaConfigInstance != null) {
            jmxClient.invoke(metaConfigInstance.getObjectName(), "reload", new Object[0], new String[0]);
            jmxClient.close();
            this.println("invoke " + metaConfigInstance.getClassName() + "#reload success");
        }
        else {
            this.println("没有找到 " + METACONFIG_NAME);
        }
    }
}