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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;
import com.taobao.metamorphosis.tools.utils.JMXClient;


/**
 * <pre>
 * 关闭一个topic的指定分区
 * usage:
 *      ClosePartitionsTool -topic xxtopic -start 2 -end 5
 *      ClosePartitionsTool -topic xxtopic -start 2 -end 5 -host 10.2.2.3
 *      ClosePartitionsTool -topic xxtopic -start 2 -end 5 -port 9999
 *      ClosePartitionsTool -topic xxtopic -start 2 -end 5 -host 10.2.2.3 -port 9999
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-23 下午3:42:38
 */

public class ClosePartitionsTool extends ShellTool {

    public ClosePartitionsTool(PrintStream out) {
        super(out);
    }


    public ClosePartitionsTool(PrintWriter writer) {
        super(writer);
    }


    public static void main(String[] args) throws Exception {
        new ClosePartitionsTool(System.out).doMain(args);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        CommandLine commandLine = this.getCommandLine(args);

        String host = commandLine.getOptionValue("host", "127.0.0.1");
        int port = Integer.parseInt(commandLine.getOptionValue("port", "9999"));
        String topic = commandLine.getOptionValue("topic");
        int start = Integer.parseInt(commandLine.getOptionValue("start"));
        int end = Integer.parseInt(commandLine.getOptionValue("end"));

        JMXClient jmxClient = JMXClient.getJMXClient(host, port);
        this.println("connected to " + jmxClient.getAddressAsString());
        ObjectInstance metaConfigInstance = jmxClient.queryMBeanForOne(METACONFIG_NAME);

        if (metaConfigInstance != null) {
            jmxClient.invoke(metaConfigInstance.getObjectName(), "closePartitions", new Object[] { topic, start, end },
                new String[] { "java.lang.String", "int", "int" });
            jmxClient.close();
            this.println("invoke " + metaConfigInstance.getClassName() + "#closePartitions success");
        }
        else {
            this.println("没有找到 " + METACONFIG_NAME);
        }
    }


    private CommandLine getCommandLine(String[] args) {
        Option topicOption = new Option("topic", true, "topic");
        topicOption.setRequired(true);
        Option startOption = new Option("start", true, "start partition num");
        startOption.setRequired(true);
        Option endOption = new Option("end", true, "end partition num");
        endOption.setRequired(true);
        return CommandLineUtils.parseCmdLine(args, new Options().addOption(topicOption).addOption(startOption)
            .addOption(endOption).addOption("host", true, "host").addOption("port", true, "port"));
    }
}