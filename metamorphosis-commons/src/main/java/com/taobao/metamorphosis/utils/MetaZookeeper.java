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
package com.taobao.metamorphosis.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Cluster;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.cluster.json.TopicBroker;


/**
 * Meta与zookeeper交互的辅助类
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-15
 * 
 */
public class MetaZookeeper {

    static{
        if(Thread.getDefaultUncaughtExceptionHandler()==null){
            Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger.warn("Thread terminated with exception: "+ t.getName(),e);
                }
            });
        }
    }

    private volatile ZkClient zkClient;

    private static Log logger = LogFactory.getLog(MetaZookeeper.class);
    public final String metaRoot;
    public final String consumersPath;
    public final String brokerIdsPath;
    @Deprecated
    public final String brokerTopicsPath;
    // added by dennis,sinace 1.4.3
    public final String brokerTopicsPubPath;
    public final String brokerTopicsSubPath;


    public ZkClient getZkClient() {
        return this.zkClient;
    }


    public void setZkClient(final ZkClient zkClient) {
        this.zkClient = zkClient;
    }


    public MetaZookeeper(final ZkClient zkClient, final String root) {
        this.zkClient = zkClient;
        this.metaRoot = this.normalize(root);
        this.consumersPath = this.metaRoot + "/consumers";
        this.brokerIdsPath = this.metaRoot + "/brokers/ids";
        this.brokerTopicsPath = this.metaRoot + "/brokers/topics";
        this.brokerTopicsPubPath = this.metaRoot + "/brokers/topics-pub";
        this.brokerTopicsSubPath = this.metaRoot + "/brokers/topics-sub";
    }


    private String normalize(final String root) {
        if (root.startsWith("/")) {
            return this.removeLastSlash(root);
        }
        else {
            return "/" + this.removeLastSlash(root);
        }
    }


    private String removeLastSlash(final String root) {
        if (root.endsWith("/")) {
            return root.substring(0, root.lastIndexOf("/"));
        }
        else {
            return root;
        }
    }

    public class ZKGroupDirs {
        public ZKGroupDirs(final String group) {
            this.consumerGroupDir = this.consumerDir + "/" + group;
            this.consumerRegistryDir = this.consumerGroupDir + "/ids";
        }

        public String consumerDir = MetaZookeeper.this.consumersPath;
        public String consumerGroupDir;
        public String consumerRegistryDir;
    }

    public class ZKGroupTopicDirs extends ZKGroupDirs {
        public ZKGroupTopicDirs(final String topic, final String group) {
            super(group);
            this.consumerOffsetDir = this.consumerGroupDir + "/offsets/" + topic;
            this.consumerOwnerDir = this.consumerGroupDir + "/owners/" + topic;
        }

        public String consumerOffsetDir;
        public String consumerOwnerDir;
    }


    /**
     * 返回broker集群,包含slave和master
     * 
     * @param zkClient
     * @return
     */
    public Cluster getCluster() {
        final Cluster cluster = new Cluster();
        final List<String> nodes = ZkUtils.getChildren(this.zkClient, this.brokerIdsPath);
        for (final String node : nodes) {
            // String brokerZKString = readData(zkClient, brokerIdsPath + "/" +
            // node);
            final int brokerId = Integer.parseInt(node);
            final Set<Broker> brokers = this.getBrokersById(brokerId);
            if (brokers != null && !brokers.isEmpty()) {
                cluster.addBroker(brokerId, brokers);
            }
        }
        return cluster;
    }


    /**
     * 从zk查询一个id下的brokers,包含master和一个或多个slave
     * */
    public Set<Broker> getBrokersById(final int brokerId) {
        final Set<Broker> set = new HashSet<Broker>();
        final Broker masterBroker = this.getMasterBrokerById(brokerId);
        final Set<Broker> slaveBrokers = this.getSlaveBrokersById(brokerId);
        if (masterBroker != null) {
            set.add(masterBroker);
        }
        if (slaveBrokers != null && !slaveBrokers.isEmpty()) {
            set.addAll(slaveBrokers);
        }
        return set;
    }


    /**
     * 从zk查询master broker,不存在则返回null
     * */
    public Broker getMasterBrokerById(final int brokerId) {
        final String brokersString = ZkUtils.readDataMaybeNull(this.zkClient, this.brokerIdsPathOf(brokerId, -1));
        if (StringUtils.isNotBlank(brokersString)) {
            return new Broker(brokerId, brokersString);
        }
        return null;
    }


    /**
     * 从zk查询slave broker,不存在则返回null
     * */
    private Set<Broker> getSlaveBrokersById(final int brokerId) {
        final Set<Broker> ret = new HashSet<Broker>();
        final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerIdsPath + "/" + brokerId);
        if (brokers == null) {
            return ret;
        }
        for (final String broker : brokers) {
            if (broker.startsWith("slave")) {
                int slaveId = -1;
                try {
                    slaveId = Integer.parseInt(broker.substring(5));
                    if (slaveId < 0) {
                        logger.warn("skip invalid slave path:" + broker);
                        continue;
                    }
                }
                catch (final Exception e) {
                    logger.warn("skip invalid slave path:" + broker);
                    continue;
                }
                final String brokerData =
                        ZkUtils.readDataMaybeNull(this.zkClient, this.brokerIdsPath + "/" + brokerId + "/" + broker);
                if (StringUtils.isNotBlank(brokerData)) {
                    ret.add(new Broker(brokerId, brokerData + "?slaveId=" + slaveId));
                }
            }
        }
        return ret;
    }


    /**
     * 返回发布了指定的topic的所有master brokers
     * */
    public Map<Integer, String> getMasterBrokersByTopic(final String topic) {
        final Map<Integer, String> ret = new TreeMap<Integer, String>();
        final List<String> brokerIds = ZkUtils.getChildren(this.zkClient, this.brokerTopicsPubPath + "/" + topic);
        if (brokerIds == null) {
            return ret;
        }
        for (final String brokerIdStr : brokerIds) {
            if (!brokerIdStr.endsWith("-m")) {
                continue;
            }
            final int brokerId = Integer.parseInt(StringUtils.split(brokerIdStr, "-")[0]);
            final Broker broker = this.getMasterBrokerById(brokerId);
            if (broker != null) {
                ret.put(brokerId, broker.getZKString());
            }
        }
        return ret;

    }


    /**
     * 返回master的topic到partition映射的map
     * 
     * @param zkClient
     * @param topics
     * @return
     */
    public Map<String, List<Partition>> getPartitionsForTopicsFromMaster(final Collection<String> topics) {
        final Map<String, List<Partition>> ret = new HashMap<String, List<Partition>>();
        for (final String topic : topics) {
            List<Partition> partList = null;
            final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerTopicsPubPath + "/" + topic);
            for (final String broker : brokers) {
                final String[] brokerStrs = StringUtils.split(broker, "-");
                if (this.isMaster(brokerStrs)) {
                    String path = this.brokerTopicsPubPath + "/" + topic + "/" + broker;
                    String brokerData = ZkUtils.readData(this.zkClient, path);
                    try {
                        final TopicBroker topicBroker = TopicBroker.parse(brokerData);
                        if (topicBroker == null) {
                            logger.warn("Null broker data for path:" + path);
                            continue;
                        }
                        for (int part = 0; part < topicBroker.getNumParts(); part++) {
                            if (partList == null) {
                                partList = new ArrayList<Partition>();
                            }
                            final Partition partition = new Partition(Integer.parseInt(brokerStrs[0]), part);
                            if (!partList.contains(partition)) {
                                partList.add(partition);
                            }
                        }
                    }
                    catch (Exception e) {
                        logger.error("A serious error occurred,could not parse broker data at path=" + path
                            + ",and broker data is:" + brokerData, e);
                    }
                }
            }
            if (partList != null) {
                Collections.sort(partList);
                ret.put(topic, partList);
            }
        }
        return ret;
    }


    private boolean isMaster(final String[] brokerStrs) {
        return brokerStrs != null && brokerStrs.length == 2 && brokerStrs[1].equals("m");
    }


    /**
     * 返回一个broker发布的所有topics
     * 
     * */
    public Set<String> getTopicsByBrokerIdFromMaster(final int brokerId) {
        final Set<String> set = new HashSet<String>();
        final List<String> allTopics = ZkUtils.getChildren(this.zkClient, this.brokerTopicsSubPath);
        for (final String topic : allTopics) {
            final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerTopicsSubPath + "/" + topic);
            if (brokers != null && brokers.size() > 0) {
                for (final String broker : brokers) {
                    if ((String.valueOf(brokerId) + "-m").equals(broker)) {
                        set.add(topic);
                    }
                }
            }
        }
        return set;
    }


    /**
     * 返回一个master 下的topic到partition映射的map
     * 
     * @param zkClient
     * @param topics
     * @return
     */
    public Map<String, List<Partition>> getPartitionsForSubTopicsFromMaster(final Collection<String> topics,
        final int brokerId) {
        final Map<String, List<Partition>> ret = new HashMap<String, List<Partition>>();
        if (topics != null) {
            for (final String topic : topics) {
                List<Partition> partList = null;
                final String dataString =
                        ZkUtils.readDataMaybeNull(this.zkClient, this.brokerTopicsPathOf(topic, false, brokerId, -1));
                if (StringUtils.isBlank(dataString)) {
                    continue;
                }
                try {
                    final TopicBroker topicBroker = TopicBroker.parse(dataString);
                    if (topicBroker == null) {
                        continue;
                    }
                    for (int part = 0; part < topicBroker.getNumParts(); part++) {
                        if (partList == null) {
                            partList = new ArrayList<Partition>();
                        }
                        partList.add(new Partition(brokerId, part));
                    }
                    if (partList != null) {
                        Collections.sort(partList);
                        ret.put(topic, partList);
                    }
                }
                catch (Exception e) {
                    throw new IllegalStateException("Parse data to TopicBroker failed,data is:" + dataString, e);
                }
            }
        }
        return ret;
    }


    /**
     * 返回一个master下的topic到partition映射的map
     * 
     * @param zkClient
     * @param topics
     * @return
     */
    public Map<String, List<String>> getPartitionStringsForSubTopicsFromMaster(final Collection<String> topics,
        final int brokerId) {
        final Map<String, List<String>> ret = new HashMap<String, List<String>>();
        final Map<String, List<Partition>> tmp = this.getPartitionsForSubTopicsFromMaster(topics, brokerId);
        if (tmp != null && !tmp.isEmpty()) {
            for (final Map.Entry<String, List<Partition>> each : tmp.entrySet()) {
                final String topic = each.getKey();
                List<String> list = ret.get(topic);
                if (list == null) {
                    list = new ArrayList<String>();
                }
                for (final Partition partition : each.getValue()) {
                    list.add(partition.getBrokerId() + "-" + partition.getPartition());
                }
                if (list != null) {
                    Collections.sort(list);
                    ret.put(topic, list);
                }
            }
        }
        return ret;
    }


    /**
     * 返回topic到partition映射的map. 包括master和slave的所有partitions
     * 
     * @param zkClient
     * @param topics
     * @return
     */
    public Map<String, List<String>> getPartitionStringsForSubTopics(final Collection<String> topics) {
        final Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (final String topic : topics) {
            List<String> partList = null;
            final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerTopicsSubPath + "/" + topic);
            for (final String broker : brokers) {
                final String[] tmp = StringUtils.split(broker, "-");
                if (tmp != null && tmp.length == 2) {
                    String path = this.brokerTopicsSubPath + "/" + topic + "/" + broker;
                    String brokerData = ZkUtils.readData(this.zkClient, path);
                    try {
                        final TopicBroker topicBroker = TopicBroker.parse(brokerData);
                        if (topicBroker == null) {
                            logger.warn("Null broker data for path:" + path);
                            continue;
                        }
                        for (int part = 0; part < topicBroker.getNumParts(); part++) {
                            if (partList == null) {
                                partList = new ArrayList<String>();
                            }

                            final String partitionString = tmp[0] + "-" + part;
                            if (!partList.contains(partitionString)) {
                                partList.add(partitionString);
                            }
                        }
                    }
                    catch (Exception e) {
                        logger.error("A serious error occurred,could not parse broker data at path=" + path
                            + ",and broker data is:" + brokerData, e);
                    }
                }
                else {
                    logger.warn("skip invalid topics path:" + broker);
                }
            }
            if (partList != null) {
                Collections.sort(partList);
                ret.put(topic, partList);
            }
        }
        return ret;
    }


    /**
     * brokerId 在zk上注册的path
     * 
     * @param brokerId
     * @param slaveId
     *            slave编号, 小于0表示master
     * 
     * */
    public String brokerIdsPathOf(final int brokerId, final int slaveId) {
        return this.brokerIdsPath + "/" + brokerId + (slaveId >= 0 ? "/slave" + slaveId : "/master");
    }


    /**
     * Master config file checksum path
     * 
     * @param brokerId
     * @return
     */
    public String masterConfigChecksum(final int brokerId) {
        return this.brokerIdsPath + "/" + brokerId + "/master_config_checksum";
    }


    /**
     * topic 在zk上注册的path
     * 
     * @param topic
     * @param brokerId
     * @param slaveId
     *            slave编号, 小于0表示master
     * */
    @Deprecated
    public String brokerTopicsPathOf(final String topic, final int brokerId, final int slaveId) {
        return this.brokerTopicsPath + "/" + topic + "/" + brokerId + (slaveId >= 0 ? "-s" + slaveId : "-m");
    }


    /**
     * 
     * Returns topic path in zk
     * 
     * @since 1.4.3
     * @param topic
     * @param brokerId
     * @param slaveId
     *            slave编号, 小于0表示master
     * */
    public String brokerTopicsPathOf(final String topic, boolean publish, final int brokerId, final int slaveId) {
        String parent = publish ? this.brokerTopicsPubPath : this.brokerTopicsSubPath;
        return parent + "/" + topic + "/" + brokerId + (slaveId >= 0 ? "-s" + slaveId : "-m");
    }

}