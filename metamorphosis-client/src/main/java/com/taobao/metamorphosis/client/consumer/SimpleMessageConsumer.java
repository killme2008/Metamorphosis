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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.util.ConcurrentHashSet;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.ConsumerZooKeeper.ZKLoadRebalanceListener;
import com.taobao.metamorphosis.client.consumer.SimpleFetchManager.FetchRequestRunner;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.exception.MetaOpeartionTimeoutException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.DataCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * 消息消费者基类
 * 
 * @author boyan
 * @Date 2011-4-23
 * @author wuhua
 * @Date 2011-6-26
 * 
 */
public class SimpleMessageConsumer implements MessageConsumer, InnerConsumer {
    private static final int DEFAULT_OP_TIMEOUT = 10000;

    static final Log log = LogFactory.getLog(FetchRequestRunner.class);

    private final RemotingClientWrapper remotingClient;

    private final ConsumerConfig consumerConfig;

    private final ConsumerZooKeeper consumerZooKeeper;

    private final MetaMessageSessionFactory messageSessionFactory;

    private final OffsetStorage offsetStorage;

    private final LoadBalanceStrategy loadBalanceStrategy;

    private final ProducerZooKeeper producerZooKeeper;

    private final ScheduledExecutorService scheduledExecutorService;

    private final SubscribeInfoManager subscribeInfoManager;

    private final RecoverManager recoverStorageManager;

    private final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry =
            new ConcurrentHashMap<String, SubscriberInfo>();

    private FetchManager fetchManager;

    private final ConcurrentHashSet<String> publishedTopics = new ConcurrentHashSet<String>();

    private RejectConsumptionHandler rejectConsumptionHandler;


    public SimpleMessageConsumer(final MetaMessageSessionFactory messageSessionFactory,
            final RemotingClientWrapper remotingClient, final ConsumerConfig consumerConfig,
            final ConsumerZooKeeper consumerZooKeeper, final ProducerZooKeeper producerZooKeeper,
            final SubscribeInfoManager subscribeInfoManager, final RecoverManager recoverManager,
            final OffsetStorage offsetStorage, final LoadBalanceStrategy loadBalanceStrategy) {
        super();
        this.messageSessionFactory = messageSessionFactory;
        this.remotingClient = remotingClient;
        this.consumerConfig = consumerConfig;
        this.producerZooKeeper = producerZooKeeper;
        this.consumerZooKeeper = consumerZooKeeper;
        this.offsetStorage = offsetStorage;
        this.subscribeInfoManager = subscribeInfoManager;
        this.recoverStorageManager = recoverManager;
        this.fetchManager = new SimpleFetchManager(consumerConfig, this);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.loadBalanceStrategy = loadBalanceStrategy;
        // Use local recover policy by default.
        this.rejectConsumptionHandler = new LocalRecoverPolicy(this.recoverStorageManager);
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                SimpleMessageConsumer.this.consumerZooKeeper.commitOffsets(SimpleMessageConsumer.this.fetchManager);
            }
        }, consumerConfig.getCommitOffsetPeriodInMills(), consumerConfig.getCommitOffsetPeriodInMills(),
        TimeUnit.MILLISECONDS);
    }


    public MetaMessageSessionFactory getParent() {
        return this.messageSessionFactory;
    }


    public FetchManager getFetchManager() {
        return this.fetchManager;
    }


    void setFetchManager(final FetchManager fetchManager) {
        this.fetchManager = fetchManager;
    }


    ConcurrentHashMap<String, SubscriberInfo> getTopicSubcriberRegistry() {
        return this.topicSubcriberRegistry;
    }


    @Override
    public OffsetStorage getOffsetStorage() {
        return this.offsetStorage;
    }


    @Override
    public synchronized void shutdown() throws MetaClientException {
        if (this.fetchManager.isShutdown()) {
            return;
        }
        try {
            this.fetchManager.stopFetchRunner();
            this.consumerZooKeeper.unRegisterConsumer(this.fetchManager);
            for (String topic : this.publishedTopics) {
                this.producerZooKeeper.unPublishTopic(topic, this);
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            this.scheduledExecutorService.shutdownNow();
            this.offsetStorage.close();
            // 删除本组的订阅关系
            this.subscribeInfoManager.removeGroup(this.consumerConfig.getGroup());
            this.messageSessionFactory.removeChild(this);
        }

    }


    @Override
    public MessageConsumer subscribe(final String topic, final int maxSize, final MessageListener messageListener)
            throws MetaClientException {
        return this.subscribe(topic, maxSize, messageListener, null);
    }


    @Override
    public MessageConsumer subscribe(final String topic, final int maxSize, final MessageListener messageListener,
            ConsumerMessageFilter filter) throws MetaClientException {
        this.checkState();
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic");
        }
        if (messageListener == null) {
            throw new IllegalArgumentException("Null messageListener");
        }
        // 先添加到公共管理器
        this.subscribeInfoManager.subscribe(topic, this.consumerConfig.getGroup(), maxSize, messageListener, filter);
        // 然后添加到自身的管理器
        SubscriberInfo info = this.topicSubcriberRegistry.get(topic);
        if (info == null) {
            info = new SubscriberInfo(messageListener, filter, maxSize);
            final SubscriberInfo oldInfo = this.topicSubcriberRegistry.putIfAbsent(topic, info);
            if (oldInfo != null) {
                throw new MetaClientException("Topic=" + topic + " has been subscribered");
            }
            return this;
        }
        else {
            throw new MetaClientException("Topic=" + topic + " has been subscribered");
        }
    }


    @Override
    public void appendCouldNotProcessMessage(final Message message) throws IOException {
        if (log.isInfoEnabled()) {
            log.info("Message could not process,save to local.MessageId=" + message.getId() + ",Topic="
                    + message.getTopic() + ",Partition=" + message.getPartition());
        }
        if (this.rejectConsumptionHandler != null) {
            this.rejectConsumptionHandler.rejectConsumption(message, this);
        }
    }


    private void checkState() {
        if (this.fetchManager.isShutdown()) {
            throw new IllegalStateException("Consumer has been shutdown");
        }
    }


    @Override
    public void completeSubscribe() throws MetaClientException {
        this.checkState();
        try {
            this.consumerZooKeeper.registerConsumer(this.consumerConfig, this.fetchManager,
                this.topicSubcriberRegistry, this.offsetStorage, this.loadBalanceStrategy);
        }
        catch (final Exception e) {
            throw new MetaClientException("注册订阅者失败", e);
        }
    }


    @Override
    public MessageListener getMessageListener(final String topic) {
        final SubscriberInfo info = this.topicSubcriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getMessageListener();
    }


    @Override
    public ConsumerMessageFilter getMessageFilter(final String topic) {
        final SubscriberInfo info = this.topicSubcriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getConsumerMessageFilter();
    }


    @Override
    public long offset(final FetchRequest fetchRequest) throws MetaClientException {
        final long start = System.currentTimeMillis();
        boolean success = false;
        try {
            final long currentOffset = fetchRequest.getOffset();
            final OffsetCommand offsetCmd =
                    new OffsetCommand(fetchRequest.getTopic(), this.consumerConfig.getGroup(),
                        fetchRequest.getPartition(), currentOffset, OpaqueGenerator.getNextOpaque());
            final String serverUrl = fetchRequest.getBroker().getZKString();
            final BooleanCommand booleanCmd =
                    (BooleanCommand) this.remotingClient.invokeToGroup(serverUrl, offsetCmd,
                        this.consumerConfig.getFetchTimeoutInMills(), TimeUnit.MILLISECONDS);
            switch (booleanCmd.getCode()) {
            case HttpStatus.Success:
                success = true;
                return Long.parseLong(booleanCmd.getErrorMsg());
            default:
                throw new MetaClientException(booleanCmd.getErrorMsg());
            }
        }
        catch (final MetaClientException e) {
            throw e;
        }
        catch (final TimeoutException e) {
            throw new MetaOpeartionTimeoutException("Send message timeout in "
                    + this.consumerConfig.getFetchTimeoutInMills() + " mills");
        }
        catch (final Exception e) {
            throw new MetaClientException("get offset failed,topic=" + fetchRequest.getTopic() + ",partition="
                    + fetchRequest.getPartition() + ",current offset=" + fetchRequest.getOffset(), e);
        }
        finally {
            final long duration = System.currentTimeMillis() - start;
            if (duration > 200) {
                MetaStatLog.addStatValue2(null, StatConstants.OFFSET_TIME_STAT, fetchRequest.getTopic(), duration);
            }
            if (!success) {
                MetaStatLog.addStat(null, StatConstants.OFFSET_FAILED_STAT, fetchRequest.getTopic());
            }
        }
    }


    @Override
    public MessageIterator fetch(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit)
            throws MetaClientException, InterruptedException {
        if (timeout <= 0 || timeUnit == null) {
            timeout = this.consumerConfig.getFetchTimeoutInMills();
            timeUnit = TimeUnit.MILLISECONDS;
        }
        final long start = System.currentTimeMillis();
        boolean success = false;
        try {
            final long currentOffset = fetchRequest.getOffset();
            final String serverUrl = fetchRequest.getBroker().getZKString();
            if (!this.remotingClient.isConnected(serverUrl)) {
                // Try to heal the connection.
                ZKLoadRebalanceListener listener =
                        this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
                if (listener.oldBrokerSet.contains(fetchRequest.getBroker())) {
                    this.remotingClient.connectWithRef(serverUrl, listener);
                }
                return null;
            }

            final GetCommand getCmd =
                    new GetCommand(fetchRequest.getTopic(), this.consumerConfig.getGroup(),
                        fetchRequest.getPartition(), currentOffset, fetchRequest.getMaxSize(),
                        OpaqueGenerator.getNextOpaque());
            final ResponseCommand response = this.remotingClient.invokeToGroup(serverUrl, getCmd, timeout, timeUnit);
            if (response instanceof DataCommand) {
                final DataCommand dataCmd = (DataCommand) response;
                final byte[] data = dataCmd.getData();
                // 获取的数据严重不足的时候，缩减maxSize
                if (data.length < fetchRequest.getMaxSize() / 2) {
                    fetchRequest.decreaseMaxSize();
                }
                success = true;
                return new MessageIterator(fetchRequest.getTopic(), data);
            }
            else {
                final BooleanCommand booleanCmd = (BooleanCommand) response;
                switch (booleanCmd.getCode()) {
                case HttpStatus.NotFound:
                    success = true;
                    return null;
                case HttpStatus.Forbidden:
                    success = true;
                    return null;
                case HttpStatus.Moved:
                    success = true;
                    fetchRequest.resetRetries();
                    fetchRequest.setOffset(Long.parseLong(booleanCmd.getErrorMsg()), -1, true);
                    return null;
                default:
                    throw new MetaClientException("Status:" + booleanCmd.getCode() + ",Error message:"
                            + ((BooleanCommand) response).getErrorMsg());
                }
            }

        }
        catch (final TimeoutException e) {
            throw new MetaOpeartionTimeoutException("Send message timeout in "
                    + this.consumerConfig.getFetchTimeoutInMills() + " mills");
        }
        catch (final MetaClientException e) {
            throw e;
        }
        catch (final InterruptedException e) {
            throw e;
        }
        catch (final Exception e) {
            throw new MetaClientException("get message failed,topic=" + fetchRequest.getTopic() + ",partition="
                    + fetchRequest.getPartition() + ",offset=" + fetchRequest.getOffset(), e);
        }
        finally {
            final long duration = System.currentTimeMillis() - start;
            if (duration > 200) {
                MetaStatLog.addStatValue2(null, StatConstants.GET_TIME_STAT, fetchRequest.getTopic(), duration);
            }
            if (!success) {
                MetaStatLog.addStat(null, StatConstants.GET_FAILED_STAT, fetchRequest.getTopic());
            }
        }
    }


    @Override
    public void setSubscriptions(final Collection<Subscription> subscriptions) throws MetaClientException {
        if (subscriptions == null) {
            return;
        }
        for (final Subscription subscription : subscriptions) {
            this.subscribe(subscription.getTopic(), subscription.getMaxSize(), subscription.getMessageListener());
        }
    }


    @Override
    public MessageIterator get(final String topic, final Partition partition, final long offset, final int maxSize,
            final long timeout, final TimeUnit timeUnit) throws MetaClientException, InterruptedException {
        if (!this.publishedTopics.contains(topic)) {
            this.producerZooKeeper.publishTopic(topic, this);
            this.publishedTopics.add(topic);
        }
        final Broker broker =
                new Broker(partition.getBrokerId(), this.producerZooKeeper.selectBroker(topic, partition));
        final TopicPartitionRegInfo topicPartitionRegInfo = new TopicPartitionRegInfo(topic, partition, offset);
        return this.fetch(new FetchRequest(broker, 0, topicPartitionRegInfo, maxSize), timeout, timeUnit);
    }


    @Override
    public RejectConsumptionHandler getRejectConsumptionHandler() {
        return this.rejectConsumptionHandler;
    }


    @Override
    public void setRejectConsumptionHandler(RejectConsumptionHandler rejectConsumptionHandler) {
        if (rejectConsumptionHandler == null) {
            throw new NullPointerException("Null rejectConsumptionHandler");
        }
        this.rejectConsumptionHandler = rejectConsumptionHandler;
    }


    @Override
    public ConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }


    @Override
    public MessageIterator get(final String topic, final Partition partition, final long offset, final int maxSize)
            throws MetaClientException, InterruptedException {
        return this.get(topic, partition, offset, maxSize, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Created with IntelliJ IDEA. User: dennis (xzhuang@avos.com) Date: 13-2-5
     * Time: 上午11:29
     */
    public static class DropPolicy implements RejectConsumptionHandler {
        @Override
        public void rejectConsumption(Message message, MessageConsumer messageConsumer) {
            // Drop the message.
        }
    }

    /**
     * Created with IntelliJ IDEA. User: dennis (xzhuang@avos.com) Date: 13-2-5
     * Time: 上午11:25
     */
    public static class LocalRecoverPolicy implements RejectConsumptionHandler {
        private final RecoverManager recoverManager;
        static final Log log = LogFactory.getLog(LocalRecoverPolicy.class);


        public LocalRecoverPolicy(RecoverManager recoverManager) {
            this.recoverManager = recoverManager;
        }


        @Override
        public void rejectConsumption(Message message, MessageConsumer messageConsumer) {
            try {
                this.recoverManager.append(messageConsumer.getConsumerConfig().getGroup(), message);
            }
            catch (IOException e) {
                log.error("Append message to local recover manager failed", e);
            }
        }
    }
}