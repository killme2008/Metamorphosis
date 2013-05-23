package com.taobao.metamorphosis.client.extension.spring;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;


public class MessageListenerContainerUnitTest {

    private MessageListenerContainer container;

    private MessageSessionFactory sessionFactory;

    private MessageConsumer consumer;

    private IMocksControl control;

    private JavaSerializationMessageBodyConverter messageBodyConverter;


    @Before
    public void setUp() {
        this.control = EasyMock.createControl();
        this.sessionFactory = this.control.createMock(MessageSessionFactory.class);
        this.consumer = this.control.createMock(MessageConsumer.class);
        this.container = new MessageListenerContainer();
        this.messageBodyConverter = new JavaSerializationMessageBodyConverter();
        this.container.setMessageBodyConverter(this.messageBodyConverter);
        this.container.setMessageSessionFactory(this.sessionFactory);
    }


    @Test
    public void testAfterPropertiesSet() {

    }

}
