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
package com.taobao.metamorphosis.storm.spout;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 支持metamorphosis消息消费的storm spout
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-8
 * 
 */
public class MetaSpout extends BaseRichSpout {
    private static final long serialVersionUID = 4382748324382L;
    public static final String FETCH_MAX_SIZE = "meta.fetch.max_size";

    public static final String TOPIC = "meta.topic";

    public static final int DEFAULT_MAX_SIZE = 128 * 1024;

    private transient MessageConsumer messageConsumer;

    private transient MessageSessionFactory sessionFactory;

    private final MetaClientConfig metaClientConfig;

    private final ConsumerConfig consumerConfig;

    static final Log log = LogFactory.getLog(MetaSpout.class);

    private final Scheme scheme;

    /**
     * Time in milliseconds to wait for a message from the queue if there is no
     * message ready when the topology requests a tuple (via
     * {@link #nextTuple()}).
     */
    public static final long WAIT_FOR_NEXT_MESSAGE = 1L;

    private transient ConcurrentHashMap<Long, MetaMessageWrapper> id2wrapperMap;
    private transient SpoutOutputCollector collector;

    private transient LinkedTransferQueue<MetaMessageWrapper> messageQueue;


    public MetaSpout(final MetaClientConfig metaClientConfig, final ConsumerConfig consumerConfig, final Scheme scheme) {
        super();
        this.metaClientConfig = metaClientConfig;
        this.consumerConfig = consumerConfig;
        this.scheme = scheme;
    }


    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        final String topic = (String) conf.get(TOPIC);
        if (topic == null) {
            throw new IllegalArgumentException(TOPIC + " is null");
        }
        Integer maxSize = (Integer) conf.get(FETCH_MAX_SIZE);
        if (maxSize == null) {
            log.warn("Using default FETCH_MAX_SIZE");
            maxSize = DEFAULT_MAX_SIZE;
        }
        this.id2wrapperMap = new ConcurrentHashMap<Long, MetaMessageWrapper>();
        this.messageQueue = new LinkedTransferQueue<MetaMessageWrapper>();
        try {
            this.collector = collector;
            this.setUpMeta(topic, maxSize);
        }
        catch (final MetaClientException e) {
            log.error("Setup meta consumer failed", e);
        }
    }


    private void setUpMeta(final String topic, final Integer maxSize) throws MetaClientException {
        this.sessionFactory = new MetaMessageSessionFactory(this.metaClientConfig);
        this.messageConsumer = this.sessionFactory.createConsumer(this.consumerConfig);
        this.messageConsumer.subscribe(topic, maxSize, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                final MetaMessageWrapper wrapper = new MetaMessageWrapper(message);
                MetaSpout.this.id2wrapperMap.put(message.getId(), wrapper);
                MetaSpout.this.messageQueue.offer(wrapper);
                try {
                    wrapper.latch.await();
                }
                catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // 消费失败，抛出运行时异常
                if (!wrapper.success) {
                    message.setRollbackOnly();
                }
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        }).completeSubscribe();
    }


    @Override
    public void close() {
        try {
            this.messageConsumer.shutdown();
        }
        catch (final MetaClientException e) {
            log.error("Shutdown consumer failed", e);
        }
        try {
            this.sessionFactory.shutdown();
        }
        catch (final MetaClientException e) {
            log.error("Shutdown session factory failed", e);
        }

    }


    @Override
    public void nextTuple() {
        if (this.messageConsumer != null) {
            try {

                final MetaMessageWrapper wrapper = this.messageQueue.poll(WAIT_FOR_NEXT_MESSAGE, TimeUnit.MILLISECONDS);
                if (wrapper == null) {
                    return;
                }
                final Message message = wrapper.message;
                this.collector.emit(this.scheme.deserialize(message.getData()), message.getId());
            }
            catch (final InterruptedException e) {
                // interrupted while waiting for message, big deal
            }
        }
    }


    @Override
    public void ack(final Object msgId) {
        if (msgId instanceof Long) {
            final long id = (Long) msgId;
            final MetaMessageWrapper wrapper = this.id2wrapperMap.remove(id);
            if (wrapper == null) {
                log.warn(String.format("don't know how to ack(%s: %s)", msgId.getClass().getName(), msgId));
                return;
            }
            wrapper.success = true;
            wrapper.latch.countDown();
        }
        else {
            log.warn(String.format("don't know how to ack(%s: %s)", msgId.getClass().getName(), msgId));
        }

    }


    @Override
    public void fail(final Object msgId) {
        if (msgId instanceof Long) {
            final long id = (Long) msgId;
            final MetaMessageWrapper wrapper = this.id2wrapperMap.remove(id);
            if (wrapper == null) {
                log.warn(String.format("don't know how to reject(%s: %s)", msgId.getClass().getName(), msgId));
                return;
            }
            wrapper.success = false;
            wrapper.latch.countDown();
        }
        else {
            log.warn(String.format("don't know how to reject(%s: %s)", msgId.getClass().getName(), msgId));
        }

    }


    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(this.scheme.getOutputFields());
    }


    public boolean isDistributed() {
        return true;
    }

}