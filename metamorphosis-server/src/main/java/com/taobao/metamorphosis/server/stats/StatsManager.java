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
package com.taobao.metamorphosis.server.stats;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.Constants;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.BuildProperties;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * 统计管理器
 * 
 * @author boyan
 * @Date 2011-4-22
 * @author wuhua
 * @Date 2011-9-9
 * 
 */
public class StatsManager implements Service {
    private final static Log log = LogFactory.getLog(StatsManager.class);
    private long startupTimestamp;
    private AtomicLong cmdPut;
    private AtomicLong txBegin;
    private AtomicLong txXABegin;
    private AtomicLong txCommit;
    private AtomicLong txRollback;
    private AtomicLong cmdGet;
    private AtomicLong cmdOffset;
    private AtomicLong getMiss;
    private AtomicLong getFailed;
    private AtomicLong putFailed;
    private final MessageStoreManager messageStoreManager;
    private final RemotingServer remotingServer;
    private RealTimeStat realTimeStat;
    private final MetaConfig metaConfig;
    private Set<Pattern> legalTopicPatSet = new HashSet<Pattern>();

    private final boolean startRealTimeStat = Boolean.valueOf(System.getProperty("meta.realtime.stat", "true"));


    public StatsManager(final MetaConfig metaConfig, final MessageStoreManager messageStoreManager,
            final RemotingServer remotingServer) {
        super();
        MetaStatLog.startRealTimeStat = this.startRealTimeStat;
        this.metaConfig = metaConfig;
        this.messageStoreManager = messageStoreManager;
        this.remotingServer = remotingServer;
        this.cmdPut = new AtomicLong(0);
        this.cmdGet = new AtomicLong(0);
        this.cmdOffset = new AtomicLong(0);
        this.getMiss = new AtomicLong(0);
        this.getFailed = new AtomicLong(0);
        this.putFailed = new AtomicLong(0);
        this.txBegin = new AtomicLong(0);
        this.txXABegin = new AtomicLong(0);
        this.txRollback = new AtomicLong(0);
        this.txCommit = new AtomicLong(0);
        this.realTimeStat = new RealTimeStat();
        this.legalTopicPatSet = new HashSet<Pattern>();
        this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                StatsManager.this.makeTopicsPatSet();
            }
        });

        // topic没有变化,只有统计属性发生了变化时,动态改变统计属性
        this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                StatsManager.this.makeTopicsPatSet();
            }
        });

        this.makeTopicsPatSet();
    }


    private void makeTopicsPatSet() {
        final Set<Pattern> set = new HashSet<Pattern>();
        for (final TopicConfig topicConfig : this.metaConfig.getTopicConfigMap().values()) {
            if (topicConfig.isStat()) {
                set.add(Pattern.compile(topicConfig.getTopic().replaceAll("\\*", ".*")));
            }
        }
        this.legalTopicPatSet = set;
    }


    public long getStartupTimestamp() {
        return this.startupTimestamp;
    }


    private boolean isStatTopic(final String topic) {
        for (final Pattern pat : this.legalTopicPatSet) {
            if (pat.matcher(topic).matches()) {
                return true;
            }
        }
        return false;
    }


    public String getStatsInfo(final String item) {
        final StringBuilder sb = new StringBuilder(1024);
        sb.append("STATS\r\n");
        if (StringUtils.isBlank(item)) {
            this.appendSystemStatsInfo(sb);
        }
        else if ("topics".equals(item)) {
            this.appendTopicsInfo(sb);
        }
        else if ("offsets".equals(item)) {
            this.appendOffsetInfo(sb);
        }
        else if ("realtime".equals(item)) {
            this.appendRealTime(sb);
        }
        else if ("help".equals(item)) {
            this.appendHelp(sb);
        }
        else if ("reset".equals(item)) {
            this.realTimeStat.resetStat();
            this.append(sb, "reset", "ok");
        }
        else {
            // 都认为是topic
            this.appendTopic(item, sb);
        }
        sb.append("END\r\n");
        return sb.toString();
    }


    private void appendHelp(final StringBuilder sb) {
        this.append(sb, "*EMPTY*", "Returns broker info.");
        this.append(sb, "help", "Returns help menu.");
        this.append(sb, "topics", "Returns topics statistics detail info.");
        this.append(sb, "offsets", "Returns partitions detail info.");
        this.append(sb, "realtime", "Returns realtime statistics detail info.");
        this.append(sb, "config", "Returns broker's config file content.");
    }


    private void appendTopic(final String item, final StringBuilder sb) {
        final Map<String/* topic */, ConcurrentHashMap<Integer/* partition */, MessageStore>> stores =
                this.messageStoreManager.getMessageStores();
        final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap = stores.get(item);
        long msgCount = 0;
        long bytes = 0;
        int partitionCount = 0;
        int resultCode = 0;// 0:存在这个topic, 1:不存在这个topic, 2:发布了这个topic但还没消息数据,
        if (subMap != null) {
            partitionCount = subMap.size();
            for (final MessageStore msgStore : subMap.values()) {
                if (msgStore != null) {
                    msgCount += msgStore.getMessageCount();
                    bytes += msgStore.getSizeInBytes();
                }
            }
        }
        else {
            if (!this.metaConfig.getTopics().contains(item)) {
                resultCode = 1;
            }
            else {
                resultCode = 2;
            }
        }
        this.append(sb, item);
        this.append(sb, "resultCode", resultCode);
        this.append(sb, "partitions", partitionCount);
        this.append(sb, "message_count", msgCount);
        this.append(sb, "bytes", bytes);
        this.append(sb, "topic_realtime_put",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_PUT, item));
        this.append(sb, "topic_realtime_get",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_GET, item));
        this.append(sb, "topic_realtime_offset",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_OFFSET, item));
        this.append(sb, "topic_realtime_get_miss",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.GET_MISS, item));
        this.append(sb, "topic_realtime_put_failed",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.PUT_FAILED, item));
        this.append(sb, "topic_realtime_message_size",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.MESSAGE_SIZE, item));
    }


    private void appendRealTime(final StringBuilder sb) {
        this.append(sb, "realtime_put", this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_PUT));
        this.append(sb, "realtime_get", this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_GET));
        this.append(sb, "realtime_offset", this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_OFFSET));
        this.append(sb, "realtime_get_miss", this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.GET_MISS));
        this.append(sb, "realtime_put_failed", this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.PUT_FAILED));
        this.append(sb, "realtime_message_size",
            this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.MESSAGE_SIZE));
    }


    private void appendOffsetInfo(final StringBuilder sb) {
        final Map<String/* topic */, ConcurrentHashMap<Integer/* partition */, MessageStore>> stores =
                this.messageStoreManager.getMessageStores();
        for (final Map.Entry<String, ConcurrentHashMap<Integer/* partition */, MessageStore>> entry : stores.entrySet()) {
            final String topic = entry.getKey();
            final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap = entry.getValue();
            if (subMap != null) {
                for (final Map.Entry<Integer, MessageStore> subEntry : subMap.entrySet()) {
                    final int partition = subEntry.getKey();
                    final MessageStore store = subEntry.getValue();
                    this.append(sb, topic, "part", partition, "min_offset", store.getMinOffset(), "max_offset",
                        store.getMaxOffset());
                }
            }
        }
    }

    public static class TopicStats {
        private final String topic;
        private final TopicConfig topicConfig;
        private final long messageCount;
        private final long messageBytes;
        private String puts = "NO";
        private String gets = "NO";
        private String getMissed = "NO";
        private String putFailed = "NO";
        private int partitions;


        public TopicStats(String topic, int partitions, TopicConfig topicConfig, long messageCount, long messageBytes,
                String realTimePut, String realTimeGet, String realTimeGetMissed, String realTimePutFailed) {
            super();
            this.topic = topic;
            this.partitions = partitions;
            this.topicConfig = topicConfig;
            this.messageCount = messageCount;
            this.messageBytes = messageBytes;
            if (realTimePut != null) {
                this.puts = realTimePut;
            }
            if (realTimeGet != null) {
                this.gets = realTimeGet;
            }
            if (realTimeGetMissed != null) {
                this.getMissed = realTimeGetMissed;
            }
            if (realTimePutFailed != null) {
                this.putFailed = realTimePutFailed;
            }
        }


        public int getPartitions() {
            return this.partitions;
        }


        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }


        public String getTopic() {
            return this.topic;
        }


        public TopicConfig getTopicConfig() {
            return this.topicConfig;
        }


        public long getMessageCount() {
            return this.messageCount;
        }


        public String getAvgMsgSize() {
            if (this.messageCount == 0) {
                return "N/A";
            }
            else {
                return String.valueOf(Math.round((double) this.getMessageBytes() / this.getMessageCount()));
            }
        }


        public long getMessageBytes() {
            return this.messageBytes;
        }


        public String getPuts() {
            return this.puts;
        }


        public void setPuts(String put) {
            this.puts = put;
        }


        public String getGets() {
            return this.gets;
        }


        public void setGets(String get) {
            this.gets = get;
        }


        public String getGetMissed() {
            return this.getMissed;
        }


        public void setGetMissed(String getMissed) {
            this.getMissed = getMissed;
        }


        public String getPutFailed() {
            return this.putFailed;
        }


        public void setPutFailed(String putFailed) {
            this.putFailed = putFailed;
        }


        @Override
        public String toString() {
            return "TopicStats [topic=" + this.topic + ", topicConfig=" + this.topicConfig + ", messageCount="
                    + this.messageCount + ", messageBytes=" + this.messageBytes + ", puts=" + this.puts + ", gets="
                    + this.gets + ", getMissed=" + this.getMissed + ", putFailed=" + this.putFailed + ", partitions="
                    + this.partitions + "]";
        }

    }


    public TopicStats getTopicStats(String topic) {
        final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap =
                this.messageStoreManager.getMessageStores().get(topic);
        if (subMap != null) {
            long sum = 0;
            long bytes = 0;
            int partitionCount = 0;
            if (subMap != null) {
                partitionCount = subMap.size();
                for (final MessageStore msgStore : subMap.values()) {
                    if (msgStore != null) {
                        sum += msgStore.getMessageCount();
                        bytes += msgStore.getSizeInBytes();
                    }
                }
            }
            TopicConfig topicConfig = this.metaConfig.getTopicConfig(topic);
            TopicStats stats =
                    new TopicStats(topic, partitionCount, topicConfig, sum, bytes,
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_PUT, topic),
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_GET, topic),
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.GET_MISS, topic),
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.PUT_FAILED, topic));
            return stats;
        }
        else {
            return null;
        }
    }


    public List<TopicStats> getTopicsStats() {
        List<TopicStats> result = new ArrayList<StatsManager.TopicStats>();
        final Map<String/* topic */, ConcurrentHashMap<Integer/* partition */, MessageStore>> stores =
                this.messageStoreManager.getMessageStores();
        for (final Map.Entry<String, ConcurrentHashMap<Integer/* partition */, MessageStore>> entry : stores.entrySet()) {
            final String topic = entry.getKey();
            final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap = entry.getValue();
            long sum = 0;
            long bytes = 0;
            int partitionCount = 0;
            if (subMap != null) {
                partitionCount = subMap.size();
                for (final MessageStore msgStore : subMap.values()) {
                    if (msgStore != null) {
                        sum += msgStore.getMessageCount();
                        bytes += msgStore.getSizeInBytes();
                    }
                }
            }
            TopicConfig topicConfig = this.metaConfig.getTopicConfig(topic);
            TopicStats stats =
                    new TopicStats(topic, partitionCount, topicConfig, sum, bytes,
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_PUT, topic),
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.CMD_GET, topic),
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.GET_MISS, topic),
                        this.realTimeStat.getGroupedRealTimeStatResult(StatConstants.PUT_FAILED, topic));
            result.add(stats);
        }
        return result;
    }


    private void appendTopicsInfo(final StringBuilder sb) {
        for (TopicStats stats : this.getTopicsStats()) {
            this.append(sb, stats.getTopic(), "partitions", stats.getPartitions(), "message_count", stats
                .getMessageCount(), "message_bytes", stats.getMessageBytes(), "accept_publish", stats.getTopicConfig()
                .isAcceptPublish(), "accept_subscribe", stats.getTopicConfig().isAcceptSubscribe());
        }
        final Map<String/* topic */, ConcurrentHashMap<Integer/* partition */, MessageStore>> stores =
                this.messageStoreManager.getMessageStores();
        List<String> configTopics = this.metaConfig.getTopics();
        for (String topic : configTopics) {
            if (!stores.containsKey(topic)) {
                this.append(sb, topic, "*Empty*");
            }
        }
    }


    RealTimeStat getRealTimeStat() {
        return this.realTimeStat;
    }


    private void appendSystemStatsInfo(final StringBuilder sb) {
        this.append(sb, "pid", this.getPid());
        this.append(sb, "broker_id", this.metaConfig.getBrokerId());
        this.append(sb, "port", this.getServerPort());
        this.append(sb, "uptime", this.getUpTime());
        this.append(sb, "version", this.getVersion());
        this.append(sb, "slave", this.metaConfig.isSlave());
        this.append(sb, "curr_connections", this.getCurrentConnectionCount());
        this.append(sb, "threads", this.getCurrentThreads());
        this.append(sb, "cmd_put", this.cmdPut.get());
        this.append(sb, "cmd_get", this.cmdGet.get());
        this.append(sb, "cmd_offset", this.cmdOffset.get());
        this.append(sb, "tx_begin", this.txBegin.get());
        this.append(sb, "tx_xa_begin", this.txXABegin.get());
        this.append(sb, "tx_commit", this.txCommit.get());
        this.append(sb, "tx_rollback", this.txRollback.get());
        this.append(sb, StatConstants.GET_MISS, this.getMiss.get());
        this.append(sb, StatConstants.PUT_FAILED, this.putFailed.get());
        this.append(sb, "total_messages", this.getTotalMessages());
        this.append(sb, "topics", this.getTopicCount());
        this.append(sb, "config_checksum", this.metaConfig.getConfigFileChecksum());
    }


    void append(final StringBuilder sb, final Object... values) {
        boolean wasFirst = true;
        for (final Object value : values) {
            if (wasFirst) {
                sb.append(value);
                wasFirst = false;
            }
            else {
                sb.append(" ").append(value);
            }
        }
        sb.append("\r\n");
    }


    public long getTotalMessages() {
        return this.messageStoreManager.getTotalMessagesCount();
    }


    public int getTopicCount() {
        return this.messageStoreManager.getTopicCount();
    }


    public int getCurrentConnectionCount() {
        return this.remotingServer.getConnectionCount(Constants.DEFAULT_GROUP);
    }


    public long getCmdPuts() {
        return this.cmdPut.get();
    }


    public long getCmdPutFailed() {
        return this.putFailed.get();
    }


    public long getCmdGets() {
        return this.cmdGet.get();
    }


    public long getCmdOffsets() {
        return this.cmdOffset.get();
    }


    public long getCmdGetMiss() {
        return this.getMiss.get();
    }


    public long getCmdGetFailed() {
        return this.getFailed.get();
    }


    public String getPid() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        if (name.contains("@")) {
            return name.split("@")[0];
        }
        return name;
    }


    public long getUpTime() {
        return (System.currentTimeMillis() - this.startupTimestamp) / 1000;
    }


    public String getVersion() {
        return BuildProperties.VERSION;
    }


    public int getCurrentThreads() {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }


    public int getServerPort() {
        return this.remotingServer.getInetSocketAddress().getPort();
    }


    public void statsPut(final String topic, String partition, final int c) {
        this.statsRealtimePut(c);
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.CMD_PUT, topic, partition, c);
        }
    }


    public void statsRealtimePut(final int c) {
        this.cmdPut.addAndGet(c);
    }


    public void statsGet(final String topic, final String group, final int c) {
        this.cmdGet.addAndGet(c);
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.CMD_GET, topic, group, c);
        }
    }


    public void statsOffset(final String topic, final String group, final int c) {
        this.cmdOffset.addAndGet(c);
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.CMD_OFFSET, topic, group, c);
        }
    }


    public void statsGetMiss(final String topic, final String group, final int c) {
        this.getMiss.addAndGet(c);
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.GET_MISS, topic, group, c);
        }
    }


    public void statsPutFailed(final String topic, final String partition, final int c) {
        this.putFailed.addAndGet(c);
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.PUT_FAILED, topic, partition, c);
        }
    }


    public void statsGetFailed(final String topic, final String group, final int c) {
        this.getFailed.addAndGet(c);
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.GET_FAILED, topic, group, c);
        }
    }


    public void statsMessageSize(final String topic, final int c) {
        if (this.isStatTopic(topic)) {
            MetaStatLog.addStatValue2(null, StatConstants.MESSAGE_SIZE, topic, c);
        }
    }


    public void statsTxBegin(final boolean isXA, final int c) {
        this.txBegin.addAndGet(c);
        if (isXA) {
            this.txXABegin.addAndGet(c);
        }
    }


    public void statsTxCommit(final int c) {
        this.txCommit.addAndGet(c);
    }


    public void statsTxRollback(final int c) {
        this.txRollback.addAndGet(c);
    }


    @Override
    public void dispose() {
        this.cmdPut = new AtomicLong(0);
        this.cmdGet = new AtomicLong(0);
        this.cmdOffset = new AtomicLong(0);
        this.getMiss = new AtomicLong(0);
        this.getFailed = new AtomicLong(0);
        this.putFailed = new AtomicLong(0);
        this.txBegin = new AtomicLong(0);
        this.txXABegin = new AtomicLong(0);
        this.txRollback = new AtomicLong(0);
        this.txCommit = new AtomicLong(0);
        this.realTimeStat.stop();
        this.realTimeStat = new RealTimeStat();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.metamorphosis.server.Service#init()
     */
    @Override
    public void init() {
        this.startupTimestamp = System.currentTimeMillis();
        this.realTimeStat.start();
    }

}