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
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;


/**
 * <pre>
 * 分区编号(目录)向前或向后偏移,一般用于分区迁移时
 * usage:
 *      MovePartitionFiles -dataDir /home/admin/metadata -topic xxtopic -start 5 -end 10 -offset -5
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-25 下午12:59:24
 */

public class MovePartitionFiles extends ShellTool {

    public MovePartitionFiles(PrintStream out) {
        super(out);
    }


    public static void main(String[] args) throws Exception {
        new MovePartitionFiles(System.out).doMain(args);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        CommandLine commandLine = this.getCommandLine(args);
        String dataDir = commandLine.getOptionValue("dataDir");
        String topic = commandLine.getOptionValue("topic");
        int start = Integer.parseInt(commandLine.getOptionValue("start"));
        int end = Integer.parseInt(commandLine.getOptionValue("end"));
        int offset = Integer.parseInt(commandLine.getOptionValue("offset"));

        this.checkArg(dataDir, topic, start, end, offset);

        List<File> oldPartitionPaths = this.getPartitionPaths(dataDir, topic, start, end);
        this.checkOldPartitionPaths(oldPartitionPaths);

        List<File> newPartitionPaths = this.getPartitionPaths(dataDir, topic, start + offset, end + offset);
        this.checkNewPartitionPaths(oldPartitionPaths, newPartitionPaths);

        this.rename(offset, oldPartitionPaths, newPartitionPaths);

    }


    private void rename(int offset, List<File> oldPartitionPaths, List<File> newPartitionPaths) {
        // 向前移动
        if (offset < 0) {
            for (int i = 0; i < oldPartitionPaths.size(); i++) {
                oldPartitionPaths.get(i).renameTo(newPartitionPaths.get(i));
                this.println(oldPartitionPaths.get(i).getAbsolutePath() + " rename to " + newPartitionPaths.get(i));
            }
        }
        else {// 向后移动
            for (int i = oldPartitionPaths.size() - 1; i >= 0; i--) {
                if (oldPartitionPaths.get(i).renameTo(newPartitionPaths.get(i))) {
                    this.println(oldPartitionPaths.get(i).getAbsolutePath() + " rename to " + newPartitionPaths.get(i));

                }
                else {
                    this.println(oldPartitionPaths.get(i).getAbsolutePath() + " rename to " + newPartitionPaths.get(i)
                            + " failed");
                }
            }
        }
    }


    /** 检查移动后的期望目录是否已经存在,有一个存在就不允许操作,抛出异常 */
    private void checkNewPartitionPaths(List<File> oldPartitionPaths, List<File> newPartitionPaths) {
        for (File file : newPartitionPaths) {
            if (!oldPartitionPaths.contains(file) && file.exists()) {
                throw new IllegalStateException("can not move," + "expected new dir " + file.getAbsolutePath()
                        + " exists");
            }
        }

    }


    /** 检查需要移动的目录是否存在,有一个不存在就不允许操作,抛出异常 */
    private void checkOldPartitionPaths(List<File> oldPartitionPaths) {
        for (File file : oldPartitionPaths) {
            if (!file.exists()) {
                throw new RuntimeException("can not move,old partition dir " + file.getAbsolutePath() + " not exists");
            }
        }

    }


    private List<File> getPartitionPaths(String dataDir, String topic, int start, int end) {
        List<File> oldPartitionPaths = new LinkedList<File>();
        for (int i = start; i <= end; i++) {
            oldPartitionPaths.add(new File(dataDir + File.separator + topic + "-" + i));
        }
        return oldPartitionPaths;
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
        Option offsetOption = new Option("offset", true, "分区编号向前或向后偏移量");
        offsetOption.setRequired(true);

        return CommandLineUtils.parseCmdLine(args, new Options().addOption(dataDirOption).addOption(topicOption)
            .addOption(startOption).addOption(endOption).addOption(offsetOption));
    }


    private void checkArg(String dataDir, String topic, int start, int end, int offset) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("can not move,topic is blank");
        }
        if (StringUtils.isBlank(dataDir)) {
            throw new IllegalArgumentException("can not move,dataDir is blank");
        }
        if (start < 0 || end < 0) {
            throw new IllegalArgumentException("can not move,start and end must not less than 0");
        }

        if (start > end) {
            throw new IllegalArgumentException("can not move,start less then end");
        }
        if (offset == 0) {
            throw new IllegalArgumentException("can not move,offset == 0,don’t move");
        }
        if ((start + offset) < 0) {
            throw new IllegalArgumentException("can not move,移动后最小的分区编号将小于0");
        }
    }

}