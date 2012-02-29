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
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;
import com.taobao.metamorphosis.tools.utils.JMXClient;


/**
 * <pre>
 * usage:
 *      TimetunnelPluginTool -start
 *      TimetunnelPluginTool -stop
 *      TimetunnelPluginTool -look
 * @author 无花
 * @since 2011-11-1 下午3:07:22
 * </pre>
 */

public class TimetunnelPluginTool extends ShellTool {
    final static private String TT_NAME = "com.taobao.metamorphosis.timetunnel:type=TimetunnelMessageListener,*";


    public TimetunnelPluginTool(PrintStream out) {
        super(out);
    }


    public TimetunnelPluginTool(PrintWriter writer) {
        super(writer);
    }


    public static void main(String[] args) throws Exception {
        new TimetunnelPluginTool(System.out).doMain(args);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        CommandLine commandLine = this.getCommandLine(args);
        String host = commandLine.getOptionValue("host", "127.0.0.1");
        int port = Integer.parseInt(commandLine.getOptionValue("port", "9999"));
        JMXClient jmxClient = JMXClient.getJMXClient(host, port);
        this.println("connected to " + jmxClient.getAddressAsString());
        ObjectInstance ttInstance = jmxClient.queryMBeanForOne(TT_NAME);
        if (ttInstance == null) {
            this.println("没有找到 " + TT_NAME);
            return;
        }
        if (commandLine.hasOption("start")) {
            jmxClient.invoke(ttInstance.getObjectName(), "startSub", new Object[0], new String[0]);
            this.println("invoke " + ttInstance.getClassName() + "#startSub success");
        }
        else if (commandLine.hasOption("stop")) {
            jmxClient.invoke(ttInstance.getObjectName(), "stopSub", new Object[0], new String[0]);
            this.println("invoke " + ttInstance.getClassName() + "#stopSub success");
        }
        else if (commandLine.hasOption("look")) {
            String ret = (String) jmxClient.getAttribute(ttInstance.getObjectName(), "SubStatus");
            this.println(ret);
        }
        else {
            this.println("unknown option");
        }

        jmxClient.close();
    }


    private CommandLine getCommandLine(String[] args) {
        Option startOption = new Option("start", false, "start");
        Option stopOption = new Option("stop", false, "stop");
        Option lookOption = new Option("look", false, "look status");
        OptionGroup group = new OptionGroup();
        group.addOption(startOption).addOption(stopOption).addOption(lookOption);

        return CommandLineUtils.parseCmdLine(args, new Options().addOptionGroup(group).addOption("host", true, "host")
            .addOption("port", true, "port"));
    }
}