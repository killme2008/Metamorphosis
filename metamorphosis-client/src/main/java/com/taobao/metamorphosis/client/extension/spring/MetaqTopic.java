package com.taobao.metamorphosis.client.extension.spring;

import java.util.Properties;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.LoadBalanceStrategy.Type;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * A metaq topic for consumer.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public class MetaqTopic {
    private ConsumerConfig consumerConfig = new ConsumerConfig();

    private String topic;

    private int maxBufferSize = 1024 * 1024;


    public MetaqTopic(String topic, int maxBufferSize, ConsumerConfig consumerConfig) {
        super();
        this.topic = topic;
        this.maxBufferSize = maxBufferSize;
        this.consumerConfig = consumerConfig;
    }


    public MetaqTopic() {
        super();
    }


    public int getMaxBufferSize() {
        return this.maxBufferSize;
    }


    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.consumerConfig == null ? 0 : this.consumerConfig.hashCode());
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
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
        MetaqTopic other = (MetaqTopic) obj;
        if (this.consumerConfig == null) {
            if (other.consumerConfig != null) {
                return false;
            }
        }
        else if (!this.consumerConfig.equals(other.consumerConfig)) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        }
        else if (!this.topic.equals(other.topic)) {
            return false;
        }
        return true;
    }


    public ConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }


    public void setConsumerConfig(ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }


    public String getTopic() {
        return this.topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public int getRecoverThreadCount() {
        return this.consumerConfig.getRecoverThreadCount();
    }


    public void setRecoverThreadCount(int recoverThreadCount) {
        this.consumerConfig.setRecoverThreadCount(recoverThreadCount);
    }


    public long getRecoverMessageIntervalInMills() {
        return this.consumerConfig.getRecoverMessageIntervalInMills();
    }


    public void setRecoverMessageIntervalInMills(long recoverMessageIntervalInMills) {
        this.consumerConfig.setRecoverMessageIntervalInMills(recoverMessageIntervalInMills);
    }


    public ZKConfig getZkConfig() {
        return this.consumerConfig.getZkConfig();
    }


    public void setZkConfig(ZKConfig zkConfig) {
        this.consumerConfig.setZkConfig(zkConfig);
    }


    public int getMaxFetchRetries() {
        return this.consumerConfig.getMaxFetchRetries();
    }


    public boolean isAlwaysConsumeFromMaxOffset() {
        return this.consumerConfig.isAlwaysConsumeFromMaxOffset();
    }


    public String getServerUrl() {
        return this.consumerConfig.getServerUrl();
    }


    public void setMaxFetchRetries(int maxFetchRetries) {
        this.consumerConfig.setMaxFetchRetries(maxFetchRetries);
    }


    public void setServerUrl(String serverUrl) {
        this.consumerConfig.setServerUrl(serverUrl);
    }


    public int getMaxIncreaseFetchDataRetries() {
        return this.consumerConfig.getMaxIncreaseFetchDataRetries();
    }


    public void setMaxIncreaseFetchDataRetries(int maxFetchRetriesForDataNotEnough) {
        this.consumerConfig.setMaxIncreaseFetchDataRetries(maxFetchRetriesForDataNotEnough);
    }


    public void setPartitionsInfo(Properties partitionsInfo) {
        this.consumerConfig.setPartitionsInfo(partitionsInfo);
    }


    public Properties getPartitionsInfo() {
        return this.consumerConfig.getPartitionsInfo();
    }


    public int getFetchRunnerCount() {
        return this.consumerConfig.getFetchRunnerCount();
    }


    public long getOffset() {
        return this.consumerConfig.getOffset();
    }


    public void setOffset(long offset) {
        this.consumerConfig.setOffset(offset);
    }


    public void setAlwaysConsumeFromMaxOffset(boolean always) {
        this.consumerConfig.setConsumeFromMaxOffset(always);
    }


    public String getGroup() {
        return this.consumerConfig.getGroup();
    }


    public void setGroup(String group) {
        this.consumerConfig.setGroup(group);
    }


    public String getPartition() {
        return this.consumerConfig.getPartition();
    }


    public void setPartition(String partition) {
        this.consumerConfig.setPartition(partition);
    }


    public String getConsumerId() {
        return this.consumerConfig.getConsumerId();
    }


    public void setConsumerId(String consumerId) {
        this.consumerConfig.setConsumerId(consumerId);
    }


    public long getFetchTimeoutInMills() {
        return this.consumerConfig.getFetchTimeoutInMills();
    }


    public void setFetchTimeoutInMills(long fetchTimeoutInMills) {
        this.consumerConfig.setFetchTimeoutInMills(fetchTimeoutInMills);
    }


    public long getMaxDelayFetchTimeInMills() {
        return this.consumerConfig.getMaxDelayFetchTimeInMills();
    }


    public void setMaxDelayFetchTimeInMills(long maxDelayFetchTimeInMills) {
        this.consumerConfig.setMaxDelayFetchTimeInMills(maxDelayFetchTimeInMills);
    }


    public void setFetchRunnerCount(int fetchRunnerCount) {
        this.consumerConfig.setFetchRunnerCount(fetchRunnerCount);
    }


    public long getCommitOffsetPeriodInMills() {
        return this.consumerConfig.getCommitOffsetPeriodInMills();
    }


    public void setCommitOffsetPeriodInMills(long commitOffsetPeriodInMills) {
        this.consumerConfig.setCommitOffsetPeriodInMills(commitOffsetPeriodInMills);
    }


    public Type getLoadBalanceStrategyType() {
        return this.consumerConfig.getLoadBalanceStrategyType();
    }


    public void setLoadBalanceStrategyType(Type loadBalanceStrategyType) {
        this.consumerConfig.setLoadBalanceStrategyType(loadBalanceStrategyType);
    }

}
