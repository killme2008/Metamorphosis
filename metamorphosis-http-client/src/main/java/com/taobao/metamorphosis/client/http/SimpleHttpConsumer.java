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
package com.taobao.metamorphosis.client.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.Shutdownable;
import com.taobao.metamorphosis.client.consumer.FetchManager;
import com.taobao.metamorphosis.client.consumer.FetchRequest;
import com.taobao.metamorphosis.client.consumer.InnerConsumer;
import com.taobao.metamorphosis.client.consumer.MessageIterator;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.consumer.RecoverStorageManager;
import com.taobao.metamorphosis.client.consumer.SimpleFetchManager;
import com.taobao.metamorphosis.client.consumer.SubscribeInfoManager;
import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.client.consumer.storage.MysqlOffsetStorage;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * 实现与SimpleMessageConsumer一致，除了发出请求使用HTTP协议以及client自己指定分配的静态LB方式
 * 
 */
public class SimpleHttpConsumer extends SimpleHttpClient implements Shutdownable, InnerConsumer {
    static final Log logger = LogFactory.getLog(SimpleHttpConsumer.class);

    private final HttpClientConfig config;
    private final ScheduledExecutorService scheduledExecutorService;
    private final OffsetStorage offsetStorage;
    private final SubscribeInfoManager subscribeInfoManager;
    private final RecoverStorageManager recoverStorageManager;

    private final FetchManager fetchManager;

    /**
     * 订阅的topic对应的partition,offset等信息,
     * 对于HTTP客户端没有rerebalance，在completeSubscribe之后就没有不会改动;
     * 里面记录的TopicPartitionRegInfo之offset会随时间改变而递增，不过此变量已经是Thread Safe
     */
    private final HashMap<String, HashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
            new HashMap<String, HashMap<Partition, TopicPartitionRegInfo>>();


    public SimpleHttpConsumer(final HttpClientConfig config) {
        super(config);
        this.config = config;
        this.subscribeInfoManager = new SubscribeInfoManager();
        this.offsetStorage = new MysqlOffsetStorage(config.getDataSource());
        this.recoverStorageManager = new RecoverStorageManager(new MetaClientConfig(), this.subscribeInfoManager);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                SimpleHttpConsumer.this.commitOffsets();
            }
        }, config.getCommitOffsetPeriodInMills(), config.getCommitOffsetPeriodInMills(), TimeUnit.MILLISECONDS);
        this.fetchManager = new SimpleFetchManager(config, this);
        this.fetchManager.resetFetchState();
    }


    private void commitOffsets() {
        this.offsetStorage.commitOffset(this.config.getGroup(), this.getTopicPartitionRegInfos());
    }


    private List<TopicPartitionRegInfo> getTopicPartitionRegInfos() {
        final List<TopicPartitionRegInfo> rt = new ArrayList<TopicPartitionRegInfo>();
        for (final HashMap<Partition, TopicPartitionRegInfo> subMap : this.topicRegistry.values()) {
            final Collection<TopicPartitionRegInfo> values = subMap.values();
            if (values != null) {
                rt.addAll(values);
            }
        }
        return rt;
    }


    /**
     * 获取指定topic和分区下面的消息
     * 
     * @throws MetaClientException
     */
    public MessageIterator get(final String topic, final Partition partition, final long offset, final int maxsize)
            throws MetaClientException, InterruptedException {
        this.checkRequest(topic, partition, offset, maxsize);
        final FetchRequest request =
                new FetchRequest(null, 0, new TopicPartitionRegInfo(topic, partition, offset), maxsize);
        return this.fetch(request, 10000, TimeUnit.MILLISECONDS);
    }


    final private void checkRequest(final String topic, final Partition partition, final long offset, final int maxsize)
            throws MetaClientException {
        if (topic == null || topic.isEmpty()) {
            throw new MetaClientException("Topic null");
        }

        if (offset < 0) {
            throw new MetaClientException("Offset must be positive");
        }

        if (partition == null) {
            throw new MetaClientException("partition must be positive");
        }

        if (maxsize < 0) {
            throw new MetaClientException("maxsize must be positive");
        }
    }


    /**
     * 
     * 订阅指定的消息，传入MessageListener，当有消息达到的时候主动通知MessageListener，请注意，
     * 调用此方法并不会使订阅关系立即生效， 只有在调用complete方法后才生效
     * 
     */
    public void subscribe(final String topic, final Partition partition, final int maxSize,
            final MessageListener messageListener) throws MetaClientException {
        this.checkState();
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic");
        }
        if (messageListener == null) {
            throw new IllegalArgumentException("Null messageListener");
        }
        // add it to group manager
        this.subscribeInfoManager.subscribe(topic, this.config.getGroup(), maxSize, messageListener);

        HashMap<Partition, TopicPartitionRegInfo> partitionTopicInfo = this.topicRegistry.get(topic);
        if (partitionTopicInfo == null) {
            partitionTopicInfo = new HashMap<Partition, TopicPartitionRegInfo>();
            this.topicRegistry.put(topic, partitionTopicInfo);
        }
        TopicPartitionRegInfo topicPartitionRegInfo = this.offsetStorage.load(topic, this.config.getGroup(), partition);
        if (topicPartitionRegInfo == null) {
            // 初始化的时候默认使用0,TODO 可能采用其他
            this.offsetStorage.initOffset(topic, this.config.getGroup(), partition, 0);// Long.MAX_VALUE
            topicPartitionRegInfo = new TopicPartitionRegInfo(topic, partition, 0);
        }
        partitionTopicInfo.put(partition, topicPartitionRegInfo);

        this.fetchManager.addFetchRequest(new FetchRequest(null, 0L, topicPartitionRegInfo, maxSize));
    }


    /**
     * 使得已经订阅的topic生效,此方法仅能调用一次,再次调用无效并将抛出异常
     */
    public void completeSubscribe() throws MetaClientException {
        this.checkState();
        // this.fetchManager.resetFetchState();
        this.fetchManager.startFetchRunner();
    }


    @Override
    public void shutdown() throws MetaClientException {
        super.shutdown();
        if (this.fetchManager.isShutdown()) {
            return;
        }
        try {
            this.fetchManager.stopFetchRunner();
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            this.scheduledExecutorService.shutdownNow();
            // 关闭前提交offsets
            this.commitOffsets();
            this.offsetStorage.close();
        }
    }


    @Override
    public MessageIterator fetch(final FetchRequest fetchRequest, final long timeout, final TimeUnit timeUnit)
            throws MetaClientException, InterruptedException {
        final long start = System.currentTimeMillis();
        final boolean success = false;

        // URI的风格是/get/{brokerID}?topic&partition&group, {brokerID}用于HTTP
        // LB（作为HTTP client端知道的唯一入口）选择对应的Broker server
        // 由 HTTP LB（如HAProxy）负责根据brokerID路由到对应的Borker server
        final String uri =
                "/get/" + fetchRequest.getPartitionObject().getBrokerId() + "?topic=" + fetchRequest.getTopic()
                        + "&partition=" + fetchRequest.getPartition() + "&offset=" + fetchRequest.getOffset()
                        + "&group=" + this.config.getGroup() + "&maxsize=" + fetchRequest.getMaxSize();
        final GetMethod httpget = new GetMethod(uri);
        try {
            this.httpclient.executeMethod(httpget);
            if (httpget.getStatusCode() == HttpStatus.Success) {
                return new MessageIterator(fetchRequest.getTopic(), httpget.getResponseBody());
            }
            else if (httpget.getStatusCode() == HttpStatus.NotFound) {
                return null;
            }
            else {
                throw new MetaClientException(httpget.getResponseBodyAsString());
            }

        }
        catch (final HttpException e) {
            logger.error(e.getMessage(), e);
            throw new MetaClientException(e);
        }
        catch (final IOException e) {
            logger.error(e.getMessage(), e);
            throw new MetaClientException(e);
        }
        finally {
            httpget.releaseConnection();
            final long duration = System.currentTimeMillis() - start;
            if (duration > 200) {
                MetaStatLog.addStatValue2(null, StatConstants.GET_TIME_STAT, fetchRequest.getTopic(), duration);
            }
            if (!success) {
                MetaStatLog.addStat(null, StatConstants.GET_FAILED_STAT, fetchRequest.getTopic());
            }
        }
    }


    private void checkState() {
        if (this.fetchManager.isShutdown()) {
            throw new IllegalStateException("Consumer has been shutdown");
        }
    }


    @Override
    public MessageListener getMessageListener(final String topic) {
        try {
            return this.subscribeInfoManager.getMessageListener(topic, this.config.getGroup());
        }
        catch (final MetaClientException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void appendCouldNotProcessMessage(final Message message) throws IOException {
        // 目前的处理是交给本地存储管理并重试
        this.recoverStorageManager.append(this.config.getGroup(), message);

    }


    @Override
    public long offset(final FetchRequest fetchRequest) throws MetaClientException {
        final long start = System.currentTimeMillis();
        final boolean success = false;

        // URI的风格是/offset/{brokerID}?topic&partition&group, {brokerID}用于HTTP
        // LB（作为HTTP client端知道的唯一入口）选择对应的Broker server
        final String uri =
                "/offset/" + fetchRequest.getPartitionObject().getBrokerId() + "?topic=" + fetchRequest.getTopic()
                        + "&partition=" + fetchRequest.getPartition() + "&offset=" + fetchRequest.getOffset();
        final GetMethod httpget = new GetMethod(uri);
        try {
            this.httpclient.executeMethod(httpget);
            if (httpget.getStatusCode() == HttpStatus.Success) {
                return Long.parseLong(httpget.getResponseBodyAsString());
            }
            else {
                throw new MetaClientException(httpget.getResponseBodyAsString());
            }
        }
        catch (final HttpException e) {
            logger.error(e.getMessage(), e);
            throw new MetaClientException(e);
        }
        catch (final IOException e) {
            logger.error(e.getMessage(), e);
            throw new MetaClientException(e);
        }
        finally {
            httpget.releaseConnection();
            final long duration = System.currentTimeMillis() - start;
            if (duration > 200) {
                MetaStatLog.addStatValue2(null, StatConstants.GET_TIME_STAT, fetchRequest.getTopic(), duration);
            }
            if (!success) {
                MetaStatLog.addStat(null, StatConstants.GET_FAILED_STAT, fetchRequest.getTopic());
            }
        }
    }

}