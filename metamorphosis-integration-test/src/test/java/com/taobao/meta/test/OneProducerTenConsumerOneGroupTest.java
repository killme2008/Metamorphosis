package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConcurrentLRUHashMap;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.consumer.SimpleFetchManager;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * meta集成测试_OneProducerOneConsumer
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */
public class OneProducerTenConsumerOneGroupTest extends BaseMetaTest {

    private final String topic = "test";

    @Test
    public void sendConsume() throws Exception {
        SimpleFetchManager.setMessageIdCache(new ConcurrentLRUHashMap());
        this.createProducer();
        this.producer.publish(this.topic);
        List<MetaMessageSessionFactory> sessionFactories = new ArrayList<MetaMessageSessionFactory>();
        final CountDownLatch latch = new CountDownLatch(6);
        try {
            // 发送消息
            final int count = 100;
            this.sendMessage(count, "hello", this.topic);

            for (int i = 0; i < 6; i++) {
                MetaMessageSessionFactory createdSessionFactory = new MetaMessageSessionFactory(this.metaClientConfig);
                MessageConsumer createdConsumer = createdSessionFactory.createConsumer(new ConsumerConfig("group"));
                this.subscribe(latch, count, createdConsumer);
                sessionFactories.add(createdSessionFactory);
                latch.countDown();
            }

            while (this.queue.size() < count) {
                Thread.sleep(1000);
                System.out.println("等待接收消息" + count + "条，目前接收到" + this.queue.size() + "条");
            }

            // 检查消息是否接收到并校验内容
            assertEquals(count, this.queue.size());
            if (count != 0) {
                for (final Message msg : this.messages) {
                    assertTrue(this.queue.contains(msg));
                }
            }
            this.log.info("received message count:" + this.queue.size());
        }
        finally {
            this.producer.shutdown();
            for (MetaMessageSessionFactory factory : sessionFactories) {
                factory.shutdown();
            }
        }

    }


    private void subscribe(final CountDownLatch latch, final int count, MessageConsumer messageConsumer)
            throws MetaClientException,
            InterruptedException {
        // 订阅接收消息并验证数据正确
        // 订阅接收消息
        try {
            messageConsumer.subscribe(this.topic, 1024 * 1024, new MessageListener() {

                public void recieveMessages(final Message messages) throws InterruptedException {
                    latch.await();
                    OneProducerTenConsumerOneGroupTest.this.queue.add(messages);
                }


                public Executor getExecutor() {
                    return null;
                }
            }).completeSubscribe();
        }
        catch (final MetaClientException e) {
            throw e;
        }

    }
}
