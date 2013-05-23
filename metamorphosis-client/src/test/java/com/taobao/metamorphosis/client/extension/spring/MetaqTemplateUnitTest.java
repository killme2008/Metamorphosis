package com.taobao.metamorphosis.client.extension.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;


public class MetaqTemplateUnitTest {

    private MetaqTemplate tempalte;

    private MessageSessionFactory sessionFactory;

    private MessageProducer producer;

    private IMocksControl control;

    private JavaSerializationMessageBodyConverter messageBodyConverter;


    @Before
    public void setUp() {
        this.control = EasyMock.createControl();
        this.sessionFactory = this.control.createMock(MessageSessionFactory.class);
        this.producer = this.control.createMock(MessageProducer.class);
        this.tempalte = new MetaqTemplate();
        this.messageBodyConverter = new JavaSerializationMessageBodyConverter();
        this.tempalte.setMessageBodyConverter(this.messageBodyConverter);
        this.tempalte.setMessageSessionFactory(this.sessionFactory);
    }


    @Test
    public void testSendMsg() throws Exception {
        EasyMock.expect(this.sessionFactory.createProducer()).andReturn(this.producer);
        this.producer.publish("test");
        EasyMock.expectLastCall();
        MessageBuilder builder = MessageBuilder.withTopic("test").withBody("hello world");
        SendResult rt = new SendResult(true, null, 0, null);
        EasyMock.expect(this.producer.sendMessage(builder.build(this.messageBodyConverter))).andReturn(rt);

        this.control.replay();
        assertSame(rt, this.tempalte.send(builder));
        this.control.verify();
    }


    @Test
    public void testSendMsgTwice() throws Exception {
        EasyMock.expect(this.sessionFactory.createProducer()).andReturn(this.producer);
        this.producer.publish("test");
        EasyMock.expectLastCall();
        MessageBuilder builder = MessageBuilder.withTopic("test").withBody("hello world");
        SendResult rt = new SendResult(true, null, 0, null);
        EasyMock.expect(this.producer.sendMessage(builder.build(this.messageBodyConverter))).andReturn(rt).times(2);

        this.control.replay();
        assertSame(rt, this.tempalte.send(builder));
        assertSame(rt, this.tempalte.send(builder));
        this.control.verify();
    }


    @Test
    public void testSendMsgThrowException() throws Exception {
        EasyMock.expect(this.sessionFactory.createProducer()).andReturn(this.producer);
        this.producer.publish("test");
        EasyMock.expectLastCall();
        MessageBuilder builder = MessageBuilder.withTopic("test").withBody("hello world");
        SendResult rt = new SendResult(false, null, -1, "test");
        EasyMock.expect(this.producer.sendMessage(builder.build(this.messageBodyConverter))).andThrow(
            new MetaClientException("test"));

        this.control.replay();
        SendResult sent = this.tempalte.send(builder);
        assertFalse(sent.isSuccess());
        assertNotNull(sent.getErrorMessage());
        assertEquals(-1, sent.getOffset());
        assertNull(sent.getPartition());
        this.control.verify();
    }


    @Test
    public void testSendMsgWithCallback() throws Exception {
        EasyMock.expect(this.sessionFactory.createProducer()).andReturn(this.producer);
        this.producer.publish("test");
        EasyMock.expectLastCall();
        MessageBuilder builder = MessageBuilder.withTopic("test").withBody("hello world");
        final SendResult rt = new SendResult(true, null, 0, null);
        SendMessageCallback cb = new SendMessageCallback() {

            @Override
            public void onMessageSent(SendResult result) {
                assertSame(rt, result);

            }


            @Override
            public void onException(Throwable e) {
                fail();

            }
        };
        this.producer.sendMessage(builder.build(this.messageBodyConverter), cb);
        EasyMock.expectLastCall();
        this.control.replay();

        this.tempalte.send(builder, cb);
        this.control.verify();
    }


    @Test
    public void testShareProducer() {
        this.tempalte.setShareProducer(true);
        EasyMock.expect(this.sessionFactory.createProducer()).andReturn(this.producer);
        this.producer.publish("test");
        EasyMock.expectLastCall().times(2);
        this.producer.publish("topic");
        EasyMock.expectLastCall().times(3);

        this.control.replay();
        assertSame(this.producer, this.tempalte.getOrCreateProducer("test"));
        assertSame(this.producer, this.tempalte.getOrCreateProducer("test"));
        assertSame(this.producer, this.tempalte.getOrCreateProducer("topic"));
        assertSame(this.producer, this.tempalte.getOrCreateProducer("topic"));
        assertSame(this.producer, this.tempalte.getOrCreateProducer("topic"));
        this.control.verify();
    }


    @Test
    public void testDestroy() throws Exception {
        EasyMock.expect(this.sessionFactory.createProducer()).andReturn(this.producer);
        this.producer.publish("test");
        EasyMock.expectLastCall();
        this.producer.shutdown();
        EasyMock.expectLastCall();
        this.control.replay();
        assertSame(this.producer, this.tempalte.getOrCreateProducer("test"));
        this.tempalte.destroy();
        this.control.verify();

    }
}
