package com.taobao.metamorphosis.client.extension.spring;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Message listener container.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public class MessageListenerContainer implements InitializingBean, DisposableBean {

    private MessageBodyConverter<?> messageBodyConverter;

    private Map<MetaQTopic/* topic */, DefaultMessageListener<?>> subscribers =
            new HashMap<MetaQTopic, DefaultMessageListener<?>>();

    private boolean shareConsumer = false;

    private static final Log log = LogFactory.getLog(MessageListenerContainer.class);

    private volatile MessageConsumer sharedConsumer;

    private MessageSessionFactory messageSessionFactory;

    private MetaQTopic defaultTopic;

    private MessageListener defaultMessageListener;

    private final CopyOnWriteArrayList<MessageConsumer> consumers = new CopyOnWriteArrayList<MessageConsumer>();


    /**
     * Returns the default topic
     * 
     * @return
     */
    public MetaQTopic getDefaultTopic() {
        return this.defaultTopic;
    }


    /**
     * Set the default topic when sharing consumers.
     * 
     * @param defaultTopic
     */
    public void setDefaultTopic(MetaQTopic defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    /**
     * Returns the default listener
     * 
     * @return
     */
    public MessageListener getDefaultMessageListener() {
        return this.defaultMessageListener;
    }


    /**
     * Set default message listener when sharing consumer.
     * 
     * @param defaultMessageListener
     */
    public void setDefaultMessageListener(MessageListener defaultMessageListener) {
        this.defaultMessageListener = defaultMessageListener;
    }


    protected MessageConsumer getMessageConsumer(MetaQTopic topic) throws MetaClientException {
        if (this.shareConsumer) {
            if (this.defaultTopic == null) {
                throw new IllegalArgumentException("You don't set default topic when sharing consumer.");
            }
            if (this.sharedConsumer == null) {
                synchronized (this) {
                    if (this.sharedConsumer == null) {
                        this.sharedConsumer =
                                this.messageSessionFactory.createConsumer(this.defaultTopic.getConsumerConfig());
                        if (!StringUtils.isBlank(this.defaultTopic.getTopic())) {
                            this.sharedConsumer.subscribe(this.defaultTopic.getTopic(),
                                this.defaultTopic.getMaxBufferSize(), this.defaultMessageListener);
                        }
                        this.consumers.add(this.sharedConsumer);
                    }
                }
            }
            return this.sharedConsumer;
        }
        else {
            if (this.defaultMessageListener != null || this.defaultTopic != null) {
                throw new IllegalStateException(
                        "You can't provide default topic or message listener when sharing consumer.");
            }
            MessageConsumer consumer = this.messageSessionFactory.createConsumer(topic.getConsumerConfig());
            this.consumers.add(consumer);
            return consumer;
        }
    }


    @Override
    public void destroy() throws Exception {
        if (this.sharedConsumer != null) {
            this.shutdownConsumer(this.sharedConsumer);
            this.sharedConsumer = null;
        }
        for (MessageConsumer consumer : this.consumers) {
            this.shutdownConsumer(consumer);
        }
        this.consumers.clear();
    }


    private void shutdownConsumer(MessageConsumer consumer) {
        try {
            consumer.shutdown();
        }
        catch (MetaClientException e) {
            log.error("Shutdown consumer failed", e);
        }
    }


    /**
     * Returns if share consumer between topics.When share consumer, all topics
     * will be subscribed by the default topic's group.
     * 
     * @return
     */
    public boolean isShareConsumer() {
        return this.shareConsumer;
    }


    /**
     * Set to be true if you want to share consumer between topics.When share
     * consumer, all topics will be subscribed by the default topic's group.
     * 
     * @param shareConsumer
     */
    public void setShareConsumer(boolean shareConsumer) {
        this.shareConsumer = shareConsumer;
    }


    /**
     * Set message session factory
     * 
     * @return
     */
    public MessageSessionFactory getMessageSessionFactory() {
        return this.messageSessionFactory;
    }


    /**
     * Returns the associated message session factory.
     * 
     * @param messageSessionFactory
     */
    public void setMessageSessionFactory(MessageSessionFactory messageSessionFactory) {
        this.messageSessionFactory = messageSessionFactory;
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Start to initialize message listener container.");
        if (this.subscribers != null) {
            Set<MessageConsumer> consumers = new HashSet<MessageConsumer>();
            for (Map.Entry<MetaQTopic, DefaultMessageListener<?>> entry : this.subscribers.entrySet()) {
                final MetaQTopic topic = entry.getKey();
                final MessageListener listener = entry.getValue();
                if (topic == null) {
                    throw new IllegalArgumentException("Topic is null");
                }
                if (StringUtils.isBlank(topic.getTopic())) {
                    throw new IllegalArgumentException("Blank topic");
                }
                MessageConsumer consumer = this.getMessageConsumer(topic);
                if (consumer == null) {
                    throw new IllegalStateException("Get or create consumer failed");
                }
                log.info("Subscribe topic=" + topic + " with group=" + topic.getGroup());
                consumer.subscribe(topic.getTopic(), topic.getMaxBufferSize(), listener).completeSubscribe();
                consumers.add(consumer);
            }
            for (MessageConsumer consumer : consumers) {
                consumer.completeSubscribe();
            }
        }
        log.info("Initialize message listener container successfully.");
    }


    /**
     * returns the message body converter.It's null by default.
     * 
     * @return
     */
    public MessageBodyConverter<?> getMessageBodyConverter() {
        return this.messageBodyConverter;
    }


    public Map<MetaQTopic, DefaultMessageListener<?>> getSubscribers() {
        return this.subscribers;
    }


    /**
     * Configure subscribers.
     * 
     * @param listeners
     */
    public void setSubscribers(Map<MetaQTopic, DefaultMessageListener<?>> listeners) {
        this.subscribers = listeners;
    }


    /**
     * Set message body converter.If listener doesn't have a converter,it will
     * use this one.It's null by default.
     * 
     * @param messageBodyConverter
     */
    public void setMessageBodyConverter(MessageBodyConverter<?> messageBodyConverter) {
        this.messageBodyConverter = messageBodyConverter;
    }

}
