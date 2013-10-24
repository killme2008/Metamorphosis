package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Message builder to build metaq message.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public class MessageBuilder {
    private final String topic;
    private String attribute;
    private Object body;
    private byte[] payload;


    private MessageBuilder(String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("blank topic");
        }
        this.topic = topic;
    }


    /*
     * Create a message builder with topic
     * 
     * @since 1.4.5
     */
    public static final MessageBuilder withTopic(String topic) {
        return new MessageBuilder(topic);
    }


    /**
     * Configure message attribute
     * 
     * @param attr
     * @return
     * @since 1.4.5
     */
    public final MessageBuilder withAttribute(String attr) {
        this.attribute = attr;
        return this;
    }


    /**
     * Configure message body object if the payload byte array is not exists.
     * 
     * @param obj
     * @return
     * @since 1.4.5
     */
    public final MessageBuilder withBody(Object obj) {
        if (this.payload != null) {
            throw new IllegalArgumentException("Payload is exists.");
        }
        this.body = obj;
        return this;
    }


    /**
     * Configure message payload if the message body object is not exists.
     * 
     * @param payload
     * @return
     * @since 1.4.5
     */
    public final MessageBuilder withPayload(byte[] payload) {
        if (this.body != null) {
            throw new IllegalArgumentException("Message body is exists.");
        }
        this.payload = payload;
        return this;
    }


    /**
     * Build a message.
     * 
     * @return
     * @since 1.4.5
     */
    public Message build() {
        return this.build(null);
    }


    /**
     * Build message by message body converter.
     * 
     * @param converter
     * @return
     * @since 1.4.5
     */
    public <T> Message build(MessageBodyConverter<T> converter) {
        if (StringUtils.isBlank(this.topic)) {
            throw new IllegalArgumentException("Blank topic");
        }
        if (this.body == null && this.payload == null) {
            throw new IllegalArgumentException("Empty payload");
        }
        byte[] payload = this.payload;
        if (payload == null && converter != null) {
            try {
                payload = converter.toByteArray((T) this.body);
            }
            catch (MetaClientException e) {
                throw new IllegalStateException("Convert message body failed.", e);
            }
        }

        if (payload == null) {
            throw new IllegalArgumentException("Empty payload");
        }
        return new Message(this.topic, payload, this.attribute);
    }
}
