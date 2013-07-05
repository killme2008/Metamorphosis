package com.taobao.meta.test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.XAMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;
import com.taobao.metamorphosis.consumer.MessageIterator;


/**
 * 测试事务超时功能
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-31
 * 
 */
public class OneProducerOneConsumerTxTimeoutTest extends BaseMetaTest {

    private final String topic = "meta-test";

    private final AtomicInteger formatIdIdGenerator = new AtomicInteger();


    @Override
    @Before
    public void setUp() throws Exception {
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        this.sessionFactory = new XAMetaMessageSessionFactory(metaClientConfig);
        this.startServer("server1");
        System.out.println("before run");
    }


    @Test
    public void testTxTimeout() throws Exception {
        try {
            this.producer = ((XAMessageSessionFactory) this.sessionFactory).createXAProducer();
            this.producer.publish(this.topic);

            final byte[] data = "hello world".getBytes();
            final Message msg = new Message(this.topic, data);
            final String uniqueQualifier = "testTxTimeout";
            final XAResource xares = ((XAMessageProducer) this.producer).getXAResource();
            final Xid xid = XIDGenerator.createXID(this.formatIdIdGenerator.incrementAndGet(), uniqueQualifier);
            // 设置事务超时为2秒
            xares.setTransactionTimeout(2);
            xares.start(xid, XAResource.TMNOFLAGS);

            final SendResult result = this.producer.sendMessage(msg);
            if (!result.isSuccess()) {
                xares.end(xid, XAResource.TMFAIL);
                xares.rollback(xid);
                throw new RuntimeException("Send message failed:" + result.getErrorMessage());
            }
            // 等待3秒
            xares.end(xid, XAResource.TMSUCCESS);
            Thread.sleep(3000);
            // prepare必须失败
            try {
                xares.prepare(xid);
                fail();
            }
            catch (final XAException e) {
                e.printStackTrace();
            }
            this.createConsumer("consumer-test");
            // this.consumer.subscribe(this.topic, 1024,
            // null).completeSubscribe();
            System.out.println(result.getPartition());
            final MessageIterator it = this.consumer.get(this.topic, result.getPartition(), 0, 1024);
            assertNull(it);
        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }
    }

}
