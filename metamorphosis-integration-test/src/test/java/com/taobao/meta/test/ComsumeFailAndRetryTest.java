package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageListener;


/**
 * 消息处理失败,重试但还没进入recover
 * 
 * @author 无花
 * @since 2011-11-14 下午6:45:35
 */

public class ComsumeFailAndRetryTest extends BaseMetaTest {

    private final String topic = "meta-test";


    // @Override
    // @Before
    // public void setUp() throws Exception {
    // final MetaClientConfig metaClientConfig = new MetaClientConfig();
    // metaClientConfig.setRecoverMessageIntervalInMills(2000);// recover时间短一些
    // metaClientConfig.setDiamondZKDataId(Utils.diamondZKDataId);
    // this.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
    // this.startServer("server1");
    // System.out.println("before run");
    // }

    @Test
    public void sendConsume() throws Exception {
        this.createProducer();
        this.producer.publish(this.topic);
        ConsumerConfig consumerConfig = new ConsumerConfig("group1");
        consumerConfig.setMaxFetchRetries(10);
        this.consumer = this.sessionFactory.createConsumer(consumerConfig);
        final AtomicInteger i = new AtomicInteger(0);
        try {
            // 订阅之前先发送几条消息
            int count = 2;
            final int failTimes = 2;
            assertTrue(failTimes < consumerConfig.getMaxFetchRetries());
            this.sendMessage(count, "hello", this.topic);

            this.consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

                public void recieveMessages(final Message messages) {
                    ComsumeFailAndRetryTest.this.queue.add(messages);
                    if (Arrays.equals(messages.getData(), "hello1".getBytes()) && i.get() < failTimes) {
                        i.incrementAndGet();
                        throw new RuntimeException("don't worry,just for test");
                    }
                }


                public Executor getExecutor() {
                    return null;
                }
            }).completeSubscribe();

            while (this.queue.size() < count + failTimes) {
                Thread.sleep(1000);
                System.out.println("等待接收消息" + (count + failTimes) + "条，目前接收到" + this.queue.size() + "条");
            }

            int j = 0;
            for (Message msg : this.queue) {
                if (Arrays.equals(msg.getData(), "hello1".getBytes())) {
                    ++j;
                }
            }
            assertEquals(j, failTimes + 1);

        }
        finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }
}
