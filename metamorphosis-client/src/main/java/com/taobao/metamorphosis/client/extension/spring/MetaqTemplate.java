package com.taobao.metamorphosis.client.extension.spring;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.beans.factory.DisposableBean;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ThreadUtils;


/**
 * Helper class that simplifies synchronous MetaQ access code.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class MetaqTemplate implements DisposableBean {
    private MessageSessionFactory messageSessionFactory;

    private String defaultTopic;

    private MessageBodyConverter<?> messageBodyConverter;

    private boolean shareProducer = false;

    private volatile MessageProducer sharedProducer;


    /**
     * returns if share a message producer between topics.It's false by default.
     * 
     * @return
     * @since 1.4.5
     */
    public boolean isShareProducer() {
        return this.shareProducer;
    }


    /**
     * If true, the template will share a message producer between topics.It's
     * false by default.
     * 
     * @param producerPerTopic
     * @since 1.4.5
     */
    public void setShareProducer(boolean producerPerTopic) {
        this.shareProducer = producerPerTopic;
    }

    private final ConcurrentHashMap<String/* topic */, FutureTask<MessageProducer>> producers =
            new ConcurrentHashMap<String, FutureTask<MessageProducer>>();


    /**
     * Returns the default topic for producers.
     * 
     * @return
     * @since 1.4.5
     */
    public String getDefaultTopic() {
        return this.defaultTopic;
    }


    /**
     * Returns the message body converter.The default is an instance of
     * JavaSerializationMessageBodyConverter.
     * 
     * @return
     */
    public MessageBodyConverter<?> getMessageBodyConverter() {
        return this.messageBodyConverter;
    }


    /**
     * Set message body converter.
     * 
     * @param messageBodyConverter
     * @since 1.4.5
     */
    public void setMessageBodyConverter(MessageBodyConverter<?> messageBodyConverter) {
        if (messageBodyConverter == null) {
            throw new IllegalArgumentException("Null messageBodyConverter");
        }
        this.messageBodyConverter = messageBodyConverter;
    }


    /**
     * Set the default topic for producers.
     * 
     * @param defaultTopic
     * @since 1.4.5
     */
    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    /**
     * Returns the associated message session factory.
     * 
     * @return
     * @since 1.4.5
     */
    public MessageSessionFactory getMessageSessionFactory() {
        return this.messageSessionFactory;
    }


    /**
     * Set message session factory fot this template.
     * 
     * @param messageSessionFactory
     * @since 1.4.5
     */
    public void setMessageSessionFactory(MessageSessionFactory messageSessionFactory) {
        if (messageSessionFactory == null) {
            throw new IllegalArgumentException("Null messageSessionFactory");
        }
        this.messageSessionFactory = messageSessionFactory;
    }


    /**
     * Returns or create a message producer for topic.
     * 
     * @param topic
     * @return
     * @since 1.4.5
     */
    public MessageProducer getOrCreateProducer(final String topic) {
        if (!this.shareProducer) {
            FutureTask<MessageProducer> task = this.producers.get(topic);
            if (task == null) {
                task = new FutureTask<MessageProducer>(new Callable<MessageProducer>() {

                    @Override
                    public MessageProducer call() throws Exception {
                        MessageProducer producer = MetaqTemplate.this.messageSessionFactory.createProducer();
                        producer.publish(topic);
                        if (!StringUtils.isBlank(MetaqTemplate.this.defaultTopic)) {
                            producer.setDefaultTopic(MetaqTemplate.this.defaultTopic);
                        }
                        return producer;
                    }

                });
                FutureTask<MessageProducer> oldTask = this.producers.putIfAbsent(topic, task);
                if (oldTask != null) {
                    task = oldTask;
                }
                else {
                    task.run();
                }
            }

            try {
                MessageProducer producer = task.get();
                return producer;
            }
            catch (ExecutionException e) {
                throw ThreadUtils.launderThrowable(e.getCause());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        else {
            if (this.sharedProducer == null) {
                synchronized (this) {
                    if (this.sharedProducer == null) {
                        this.sharedProducer = this.messageSessionFactory.createProducer();
                        if (!StringUtils.isBlank(this.defaultTopic)) {
                            this.sharedProducer.setDefaultTopic(this.defaultTopic);
                        }
                    }
                }
            }
            this.sharedProducer.publish(topic);
            return this.sharedProducer;
        }
        throw new IllegalStateException("Could not create producer for topic '" + topic + "'");
    }


    /**
     * Send message built by message builder.Returns the sent result.
     * 
     * @param builder
     * @return
     * @throws InterruptedException
     * @since 1.4.5
     */
    public SendResult send(MessageBuilder builder, long timeout, TimeUnit unit) throws InterruptedException {
        Message msg = builder.build(this.messageBodyConverter);
        final String topic = msg.getTopic();
        MessageProducer producer = this.getOrCreateProducer(topic);
        try {
            return producer.sendMessage(msg, timeout, unit);
        }
        catch (MetaClientException e) {
            return new SendResult(false, null, -1, ExceptionUtils.getFullStackTrace(e));
        }
    }


    /**
     * Send message built by message builder.Returns the sent result.
     * 
     * @param builder
     * @return
     * @throws InterruptedException
     * @since 1.4.5
     */
    public SendResult send(MessageBuilder builder) throws InterruptedException {
        Message msg = builder.build(this.messageBodyConverter);
        final String topic = msg.getTopic();
        MessageProducer producer = this.getOrCreateProducer(topic);
        try {
            return producer.sendMessage(msg);
        }
        catch (MetaClientException e) {
            return new SendResult(false, null, -1, ExceptionUtils.getFullStackTrace(e));
        }
    }


    /**
     * Send message asynchronously with callback.
     * 
     * @param builder
     * @param cb
     * @param timeout
     * @param unit
     * @since 1.4.5
     */
    public void send(MessageBuilder builder, SendMessageCallback cb, long timeout, TimeUnit unit) {
        Message msg = builder.build(this.messageBodyConverter);
        final String topic = msg.getTopic();
        MessageProducer producer = this.getOrCreateProducer(topic);
        producer.sendMessage(msg, cb, timeout, unit);
    }


    @Override
    public void destroy() throws Exception {
        if (this.sharedProducer != null) {
            this.sharedProducer.shutdown();
            this.sharedProducer = null;
        }
        for (FutureTask<MessageProducer> task : this.producers.values()) {
            try {
                MessageProducer producer = task.get(5000, TimeUnit.MILLISECONDS);
                if (producer != null) {
                    producer.shutdown();
                }
            }
            catch (Exception e) {
                // ignore
            }
        }
        this.producers.clear();

    }


    /**
     * Send message asynchronously with callback.
     * 
     * @param builder
     * @param cb
     * @since 1.4.5
     */
    public void send(MessageBuilder builder, SendMessageCallback cb) {
        Message msg = builder.build(this.messageBodyConverter);
        final String topic = msg.getTopic();
        MessageProducer producer = this.getOrCreateProducer(topic);
        producer.sendMessage(msg, cb);
    }

}
