package com.taobao.meta.test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.XAMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;


/**
 * 测试本地事务发送消息
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-31
 * 
 */
public class XATxTenProducerTenConsumerTenGroupTest extends BaseMetaTest {

    private final String topic = "meta-test";
    private final String UNIQUE_QUALIFIER = "XATxTenProducerTenConsumerTenGroupTest";

    private final AtomicInteger formatIdIdGenerator = new AtomicInteger();


    @Override
    @Before
    public void setUp() throws Exception {
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        this.sessionFactory = new XAMetaMessageSessionFactory(metaClientConfig);
        this.startServer("server1");
        System.out.println("before run");
    }


    public void create_nXAProducer(final int num) {
        this.producerList = new ArrayList<MessageProducer>();
        for (int i = 0; i < num; i++) {
            this.producerList.add(i, ((XAMessageSessionFactory) this.sessionFactory).createXAProducer());
        }
    }


    public void xaTxSendMessage_nProducer(final int count, final String strdata, final String topic, final int num)
            throws Exception {
        this.messages = new ArrayList<Message>();
        for (int j = 0; j < num; j++) {
            // 需要发布topic
            final XAMessageProducer messageProducer = (XAMessageProducer) this.producerList.get(j);
            messageProducer.publish(topic);

            for (int i = 0; i < count; i++) {
                final byte[] data = ("hello" + j + i).getBytes();
                final Message msg = new Message(topic, data);
                final XAResource xares = messageProducer.getXAResource();
                final Xid xid =
                        XIDGenerator.createXID(this.formatIdIdGenerator.incrementAndGet(), this.UNIQUE_QUALIFIER);
                xares.start(xid, XAResource.TMNOFLAGS);
                final SendResult result = messageProducer.sendMessage(msg);
                if (!result.isSuccess() || i % 2 == 0) {
                    xares.end(xid, XAResource.TMFAIL);
                    xares.rollback(xid);
                }
                else {
                    xares.end(xid, XAResource.TMSUCCESS);
                    xares.prepare(xid);
                    xares.commit(xid, false);
                    this.messages.add(msg);
                }
            }
        }
    }


    @Test
    public void sendConsume() throws Exception {

        this.create_nXAProducer(10);

        try {
            // 发送消息，但是一半失败
            final int count = 10;
            this.xaTxSendMessage_nProducer(count, "hello", this.topic, 10);
            // 订阅接收消息并验证数据正确
            this.subscribe_nConsumer(this.topic, 1024 * 1024, count / 2, 10, 10);
        }
        catch (final Throwable e) {
            e.printStackTrace();
        }
        finally {
            for (int i = 0; i < 10; i++) {
                this.producerList.get(i).shutdown();
                this.consumerList.get(i).shutdown();
            }
        }
    }
}
