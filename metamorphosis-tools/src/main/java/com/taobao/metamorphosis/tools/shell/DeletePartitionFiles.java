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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;


/**
 * <pre>
 * 清除一个topic的某些分区数据
 * usage:
 *      DeletePartitionFiles -dataDir /home/admin/metadata -topic xxtopic -start 5 -end 10
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-25 上午11:04:16
 */

public class DeletePartitionFiles extends ShellTool {

    public DeletePartitionFiles(PrintStream out) {
        super(out);
    }


    public static void main(String[] args) throws Exception {
        new DeletePartitionFiles(System.out).doMain(args);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        CommandLine commandLine = this.getCommandLine(args);
        String dataDir = commandLine.getOptionValue("dataDir");
        String topic = commandLine.getOptionValue("topic");
        int start = Integer.parseInt(commandLine.getOptionValue("start"));
        int end = Integer.parseInt(commandLine.getOptionValue("end"));

        this.checkArg(dataDir, topic, start, end);

        File partitionDir = null;
        for (int i = start; i <= end; i++) {
            try {
                partitionDir = new File(dataDir + File.separator + topic + "-" + i);

                if (!commandLine.hasOption("f")) {
                    String in;
                    do {
                        System.out.println("delete " + partitionDir.getAbsolutePath() + "?y/n");
                        in = sc.next();

                    } while (in == null || (!in.equalsIgnoreCase("n") && !in.equalsIgnoreCase("y")));

                    if (!in.equalsIgnoreCase("y")) {
                        continue;
                    }
                }

                if (partitionDir.exists()) {
                    try {
                        FileUtils.deleteDirectory(partitionDir);
                        System.out.println("delete " + partitionDir.getAbsolutePath() + " successed");
                    }
                    catch (IOException e) {
                        System.out.println("delete " + partitionDir.getAbsolutePath() + " failed");
                    }
                }
                else {
                    System.out.println(partitionDir.getAbsolutePath() + " not exists");
                }
            }
            catch (Exception e) {
                System.out.println("error occured when delete " + partitionDir.getAbsolutePath());
                e.printStackTrace();
            }
        }
        sc.close();

    }


    private CommandLine getCommandLine(String[] args) {
        Option dataDirOption = new Option("dataDir", true, "meta data dir");
        dataDirOption.setRequired(true);
        Option topicOption = new Option("topic", true, "topic");
        topicOption.setRequired(true);
        Option startOption = new Option("start", true, "start partition number");
        startOption.setRequired(true);
        Option endOption = new Option("end", true, "end partition number");
        endOption.setRequired(true);
        Option forceOption = new Option("f", false, "是否需要确认");

        return CommandLineUtils.parseCmdLine(args, new Options().addOption(dataDirOption).addOption(topicOption)
            .addOption(startOption).addOption(endOption).addOption(forceOption));
    }


    private void checkArg(String dataDir, String topic, int start, int end) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("can not delete,topic is blank");
        }
        if (StringUtils.isBlank(dataDir)) {
            throw new IllegalArgumentException("can not delete,dataDir is blank");
        }
        if (start < 0 || end < 0) {
            throw new IllegalArgumentException("can not delete,start and end must not less than 0");
        }

        if (start > end) {
            throw new IllegalArgumentException("can not delete,start less then end");
        }
    }

}