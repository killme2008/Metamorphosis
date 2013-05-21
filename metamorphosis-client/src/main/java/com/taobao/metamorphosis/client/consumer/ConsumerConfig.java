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
package com.taobao.metamorphosis.client.consumer;

import com.taobao.metamorphosis.client.MetaClientConfig;


/**
 * 消费者配置，主要配置选项如下：
 * <ul>
 * <li>group:分组名称，必须，表示该消费者所在分组，同一分组的消费者正常情况下不会接收重复消息，共同消费某一topic</li>
 * <li>consumerId: 消费者id，用于唯一标识一个消费者，可不设置，系统会根据分组名称自动生成</li>
 * <li>commitOffsetPeriodInMills: 保存offset的时间间隔，默认5秒，单位毫秒</li>
 * <li>fetchTimeoutInMills: 同步获取消息的默认超时时间，默认10秒，单位毫秒</li>
 * <li>maxDelayFetchTimeInMills: 当获取消息失败的时候（包括get
 * miss或者任何异常情况)会延迟获取，此值设置最大的延迟时间，单位毫秒</li>
 * <li>fetchRunnerCount: 获取消息的线程数，默认cpu个。</li>
 * <li>partition:当使用直连模式时，此值指定连接的分区，形如"brokerId-partition"的字符串</li>
 * <li>offset:指定读取的offset偏移量,默认从0开始</li>
 * <li>maxFetchRetries:同一条消息在处理失败情况下最大重试次数，默认5次，超过就跳过这条消息并记录</li>
 * <li>maxIncreaseFetchDataRetries:拉取数据重试次数超过这个值,则增长每次拉取的数据量</li>
 * <li>loadBalanceStrategyType: 消费者负载均衡策略</li>
 * </ul>
 * 
 * @author boyan
 * @Date 2011-4-28
 * @author wuhua
 * 
 */
public class ConsumerConfig extends MetaClientConfig {
    static final long serialVersionUID = -1L;
    private int fetchRunnerCount = Runtime.getRuntime().availableProcessors();
    private long maxDelayFetchTimeInMills = 5000;
    @Deprecated
    private long maxDelayFetchTimeWhenExceptionInMills = 10000;
    private long fetchTimeoutInMills = 10000;
    private String consumerId;
    private String partition;
    private long offset = 0;
    private String group;
    private long commitOffsetPeriodInMills = 5000L;
    private int maxFetchRetries = 5;
    private boolean alwaysConsumeFromMaxOffset = false;
    private LoadBalanceStrategy.Type loadBalanceStrategyType = LoadBalanceStrategy.Type.DEFAULT;

    // 把消息处理失败重试跟拉取数据不足重试分开,
    // 因为有时不需要处理失败重试(maxFetchRetries设为maxIntValue),
    // 但需要自增长拉取的数据量
    private int maxIncreaseFetchDataRetries = 5;


    public int getMaxFetchRetries() {
        return this.maxFetchRetries;
    }


    public boolean isAlwaysConsumeFromMaxOffset() {
        return this.alwaysConsumeFromMaxOffset;
    }


    public void setMaxFetchRetries(final int maxFetchRetries) {
        this.maxFetchRetries = maxFetchRetries;
    }


    /**
     * 拉取数据重试次数超过这个值,则增长每次拉取的数据量
     * 
     * @return
     */
    public int getMaxIncreaseFetchDataRetries() {
        return this.maxIncreaseFetchDataRetries;
    }


    /**
     * 设置拉取数据重试次数超过这个值,则增长每次拉取的数据量
     * 
     * @param maxFetchRetriesForDataNotEnough
     */
    public void setMaxIncreaseFetchDataRetries(final int maxFetchRetriesForDataNotEnough) {
        this.maxIncreaseFetchDataRetries = maxFetchRetriesForDataNotEnough;
    }


    /**
     * 
     * @param group
     *            分组名称
     */
    public ConsumerConfig(final String group) {
        super();
        this.group = group;
    }


    /**
     * 
     * @param consumerId
     *            消费者id，如果不设置，系统会自动产生 "ip_时间"
     * @param group
     */
    public ConsumerConfig(final String consumerId, final String group) {
        super();
        this.consumerId = consumerId;
        this.group = group;
    }


    public ConsumerConfig() {
        super();
    }


    /**
     * 请求线程数，默认cpus个
     * 
     * @return
     */
    public int getFetchRunnerCount() {
        return this.fetchRunnerCount;
    }


    /**
     * 请求offset起点
     * 
     * @return
     */
    public long getOffset() {
        return this.offset;
    }


    /**
     * 设置请求offset
     * 
     * @param offset
     */
    public void setOffset(final long offset) {
        this.offset = offset;
    }


    /**
     * 设置首次订阅是否从最新位置开始消费。
     * 
     * @param offset
     */
    public void setConsumeFromMaxOffset() {
        this.setConsumeFromMaxOffset(false);
    }


    /**
     * 设置每次订阅是否从最新位置开始消费。
     * 
     * @since 1.4.5
     * @param always
     *            如果为true，表示每次启动都从最新位置开始消费。通常在测试的时候可以设置为true。
     */
    public void setConsumeFromMaxOffset(boolean always) {
        this.alwaysConsumeFromMaxOffset = always;
        this.setOffset(Long.MAX_VALUE);
    }


    /**
     * 消费者分组名
     * 
     * @return
     */
    public String getGroup() {
        return this.group;
    }


    /**
     * 设置消费者分组名
     * 
     * @param group
     *            分组名，不得为空
     */
    public void setGroup(final String group) {
        this.group = group;
    }


    /**
     * 分区，仅在直接连接服务器的时候有效
     * 
     * @return
     */
    public String getPartition() {
        return this.partition;
    }


    /**
     * 设置分区,仅在直接连接服务器的时候有效
     * 
     * @param partition
     *            形如"brokerId-partition"的字符串
     */
    public void setPartition(final String partition) {
        this.partition = partition;
    }


    /**
     * 消费者id
     * 
     * @return
     */
    public String getConsumerId() {
        return this.consumerId;
    }


    /**
     * 设置消费者id，可不设置，系统将按照"ip_时间"的规则自动产生
     * 
     * @param consumerId
     */
    public void setConsumerId(final String consumerId) {
        this.consumerId = consumerId;
    }


    /**
     * 请求超时时间，毫秒为单位，默认10秒
     * 
     * @return
     */
    public long getFetchTimeoutInMills() {
        return this.fetchTimeoutInMills;
    }


    /**
     * 设置请求超时时间，毫秒为单位，默认10秒
     * 
     * @param fetchTimeoutInMills
     *            毫秒
     */
    public void setFetchTimeoutInMills(final long fetchTimeoutInMills) {
        this.fetchTimeoutInMills = fetchTimeoutInMills;
    }


    /**
     * 请求间隔的最大时间，单位毫秒，默认5秒
     * 
     * @return
     */
    public long getMaxDelayFetchTimeInMills() {
        return this.maxDelayFetchTimeInMills;
    }


    /**
     * 设置请求间隔的最大时间，单位毫秒，默认5秒
     * 
     * @param maxDelayFetchTimeInMills
     */
    public void setMaxDelayFetchTimeInMills(final long maxDelayFetchTimeInMills) {
        this.maxDelayFetchTimeInMills = maxDelayFetchTimeInMills;
    }


    /**
     * 当请求发生异常时(例如无可用连接等),请求间隔的最大时间，单位毫秒，默认10秒
     * 
     * @deprecated 1.4开始废除，请使用maxDelayFetchTimeInMills
     * @return
     */
    @Deprecated
    public long getMaxDelayFetchTimeWhenExceptionInMills() {
        return this.maxDelayFetchTimeWhenExceptionInMills;
    }


    /**
     * 当请求发生异常时(例如无可用连接等),设置请求间隔的最大时间，单位毫秒，默认10秒
     * 
     * @deprecated 1.4开始废除，请使用maxDelayFetchTimeInMills
     * @param maxDelayFetchTimeWhenExceptionInMills
     */
    @Deprecated
    public void setMaxDelayFetchTimeWhenExceptionInMills(final long maxDelayFetchTimeWhenExceptionInMills) {
        this.maxDelayFetchTimeWhenExceptionInMills = maxDelayFetchTimeWhenExceptionInMills;
    }


    /**
     * 设置请求线程数，默认cpus个
     * 
     * @param fetchRunnerCount
     */
    public void setFetchRunnerCount(final int fetchRunnerCount) {
        this.fetchRunnerCount = fetchRunnerCount;
    }


    /**
     * 保存offset的间隔时间，单位毫秒，默认5秒
     * 
     * @return
     */
    public long getCommitOffsetPeriodInMills() {
        return this.commitOffsetPeriodInMills;
    }


    /**
     * 设置保存offset的间隔时间，单位毫秒，默认5秒
     * 
     * @param commitOffsetPeriodInMills
     *            毫秒
     */
    public void setCommitOffsetPeriodInMills(final long commitOffsetPeriodInMills) {
        this.commitOffsetPeriodInMills = commitOffsetPeriodInMills;
    }


    /**
     * 获取负载均衡策略类型
     * 
     * @return
     */
    public LoadBalanceStrategy.Type getLoadBalanceStrategyType() {
        return this.loadBalanceStrategyType;
    }


    /**
     * 设置负载均衡策略类型
     * 
     * @param loadBalanceStrategyType
     */
    public void setLoadBalanceStrategyType(final LoadBalanceStrategy.Type loadBalanceStrategyType) {
        this.loadBalanceStrategyType = loadBalanceStrategyType;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.alwaysConsumeFromMaxOffset ? 1231 : 1237);
        result = prime * result + (int) (this.commitOffsetPeriodInMills ^ this.commitOffsetPeriodInMills >>> 32);
        result = prime * result + (this.consumerId == null ? 0 : this.consumerId.hashCode());
        result = prime * result + this.fetchRunnerCount;
        result = prime * result + (int) (this.fetchTimeoutInMills ^ this.fetchTimeoutInMills >>> 32);
        result = prime * result + (this.group == null ? 0 : this.group.hashCode());
        result = prime * result + (this.loadBalanceStrategyType == null ? 0 : this.loadBalanceStrategyType.hashCode());
        result = prime * result + (int) (this.maxDelayFetchTimeInMills ^ this.maxDelayFetchTimeInMills >>> 32);
        result =
                prime
                * result
                + (int) (this.maxDelayFetchTimeWhenExceptionInMills ^ this.maxDelayFetchTimeWhenExceptionInMills >>> 32);
        result = prime * result + this.maxFetchRetries;
        result = prime * result + this.maxIncreaseFetchDataRetries;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + (this.partition == null ? 0 : this.partition.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        ConsumerConfig other = (ConsumerConfig) obj;
        if (this.alwaysConsumeFromMaxOffset != other.alwaysConsumeFromMaxOffset) {
            return false;
        }
        if (this.commitOffsetPeriodInMills != other.commitOffsetPeriodInMills) {
            return false;
        }
        if (this.consumerId == null) {
            if (other.consumerId != null) {
                return false;
            }
        }
        else if (!this.consumerId.equals(other.consumerId)) {
            return false;
        }
        if (this.fetchRunnerCount != other.fetchRunnerCount) {
            return false;
        }
        if (this.fetchTimeoutInMills != other.fetchTimeoutInMills) {
            return false;
        }
        if (this.group == null) {
            if (other.group != null) {
                return false;
            }
        }
        else if (!this.group.equals(other.group)) {
            return false;
        }
        if (this.loadBalanceStrategyType != other.loadBalanceStrategyType) {
            return false;
        }
        if (this.maxDelayFetchTimeInMills != other.maxDelayFetchTimeInMills) {
            return false;
        }
        if (this.maxDelayFetchTimeWhenExceptionInMills != other.maxDelayFetchTimeWhenExceptionInMills) {
            return false;
        }
        if (this.maxFetchRetries != other.maxFetchRetries) {
            return false;
        }
        if (this.maxIncreaseFetchDataRetries != other.maxIncreaseFetchDataRetries) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.partition == null) {
            if (other.partition != null) {
                return false;
            }
        }
        else if (!this.partition.equals(other.partition)) {
            return false;
        }
        return true;
    }

}