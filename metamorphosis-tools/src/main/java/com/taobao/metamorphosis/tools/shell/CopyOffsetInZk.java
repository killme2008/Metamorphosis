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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.query.OffsetQueryDO;
import com.taobao.metamorphosis.tools.query.OffsetQueryDO.QueryType;
import com.taobao.metamorphosis.tools.query.Query;
import com.taobao.metamorphosis.tools.utils.CommandLineUtils;


/**
 * <pre>
 * usage:
 *      CopyOffsetInZk -topic xxtopic -src 1 -target 2 -start 5 -end 10
 *      CopyOffsetInZk -topic xxtopic -src 1 -target 2 -start 5 -end 10 -targetStart 2
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-24 上午10:19:30
 */

public class CopyOffsetInZk extends ShellTool {
    private final Query query;
    private final ZkManager zkManager;


    public static void main(String[] args) throws Exception {
        new CopyOffsetInZk(System.out).doMain(args);
        System.exit(0);
    }


    CopyOffsetInZk(PrintStream out) throws InitException {
        super(out);
        this.query = new Query();
        this.query.init("zk.properties", null);
        this.zkManager = new ZkManager(this.query);
    }


    public CopyOffsetInZk(String configFile, PrintWriter out) throws InitException {
        super(out);
        this.query = new Query();
        this.query.init(configFile, null);
        this.zkManager = new ZkManager(query);
    }


    @Override
    public void doMain(String[] args) throws Exception {

        CommandLine commandLine = this.getCommandLine(args);
        String topic = commandLine.getOptionValue("topic");
        int src = Integer.parseInt(commandLine.getOptionValue("src"));
        int target = Integer.parseInt(commandLine.getOptionValue("target"));
        int start = Integer.parseInt(commandLine.getOptionValue("start"));
        int end = Integer.parseInt(commandLine.getOptionValue("end"));
        int targetStart = Integer.parseInt(commandLine.getOptionValue("targetStart", "0"));

        this.checkArg(topic, src, target, start, end);

        List<String> groups = this.query.getConsumerGroups(QueryType.zk);
        Map<Partition, Partition> srcTargetPartitionMap =
                this.getSrcTargetPartitionMap(this.getSourcePartitions(src, start, end), target, targetStart);
        for (String group : groups) {

            for (Map.Entry<Partition, Partition> entry : srcTargetPartitionMap.entrySet()) {
                Partition oldPartition = entry.getKey();
                Partition newPartition = entry.getValue();

                String srcOffset = this.query.queryOffset(new OffsetQueryDO(topic, group, oldPartition.toString(), "zk"));
                if (!StringUtils.isBlank(srcOffset)) {
                    if (!StringUtils.isBlank(this.query.queryOffset(new OffsetQueryDO(topic, group, newPartition.toString(), "zk")))) {
                        this.println("topic=" + topic + ",group=" + group + ",partition[" + newPartition
                                + "] offset 已经存在");
                        continue;
                    }
                    this.zkManager.setOffset(topic, group, newPartition, srcOffset);
                    this.println("copy offset successed for topic[" + topic + "], group=" + group + ", " + oldPartition
                            + "-->" + newPartition + ", offset=" + srcOffset);
                }
                else {
                    this.println("topic=" + topic + ",group=" + group + ",partition[" + oldPartition
                            + "] offset 不存在或查询有误");
                }
            }
        }
    }


    //
    // private Map<String, String> getOffsetPathMap(String group, String topic,
    // Map<Partition, Partition> srcTargetPartitionMap) {
    //
    // Map<String, String> map = new LinkedHashMap<String, String>();
    //
    // for (Map.Entry<Partition, Partition> entry :
    // srcTargetPartitionMap.entrySet()) {
    // map.put(Query.getOffsetPath(group, topic, entry.getKey()),
    // Query.getOffsetPath(group, topic, entry.getValue()));
    // }
    // return map;
    // }

    private List<Partition> getSourcePartitions(int brokerId, int startPartitionNo, int endPartitionNo) {
        List<Partition> list = new LinkedList<Partition>();
        for (int i = startPartitionNo; i <= endPartitionNo; i++) {
            list.add(new Partition(brokerId, i));
        }
        return list;
    }


    private Map<Partition, Partition> getSrcTargetPartitionMap(List<Partition> srcPartitions, int target, int start) {
        Map<Partition, Partition> map = new TreeMap<Partition, Partition>();
        for (int i = 0; i < srcPartitions.size(); i++) {
            map.put(srcPartitions.get(i), new Partition(target, i + start));
        }
        return map;
    }


    private CommandLine getCommandLine(String[] args) {
        Option topicOption = new Option("topic", true, "topic");
        topicOption.setRequired(true);
        Option fromOption = new Option("src", true, "source broker id");
        fromOption.setRequired(true);
        Option toOption = new Option("target", true, "target broker id");
        toOption.setRequired(true);
        Option startOption = new Option("start", true, "start partition number");
        startOption.setRequired(true);
        Option endOption = new Option("end", true, "end partition number");
        endOption.setRequired(true);
        Option targetStartOption = new Option("targetStart", true, "目标分区的起始编号");

        return CommandLineUtils.parseCmdLine(args, new Options().addOption(topicOption).addOption(fromOption)
            .addOption(toOption).addOption(startOption).addOption(endOption).addOption(targetStartOption));
    }


    private void checkArg(String topic, int src, int target, int start, int end) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("topic is blank");
        }
        if (src < 0 || target < 0) {
            throw new IllegalArgumentException("src and target must not less than 0");
        }
        if (src == target) {
            throw new IllegalArgumentException("src equals target");
        }

        if (start > end) {
            throw new IllegalArgumentException("start less then end");
        }
    }


    Query getQuery() {
        return this.query;
    }

}