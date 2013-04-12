package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageListener;


/**
 * 消息处理失败,重试多次还是失败，进入recover
 * 
 * @author 无花
 * @since 2011-11-14 下午6:45:35
 */

public class ComsumeFailAndRecoverTest extends BaseMetaTest {

    private final String topic = "meta-test";


    @Override
    @Before
    public void setUp() throws Exception {
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setRecoverMessageIntervalInMills(2000);// recover时间短一些
        this.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        this.startServer("server1");
        System.out.println("before run");
    }


    @Test
    public void sendConsume() throws Exception {
        this.createProducer();
        this.producer.publish(this.topic);
        final ConsumerConfig consumerConfig = new ConsumerConfig("group1");
        consumerConfig.setMaxFetchRetries(5);
        this.consumer = this.sessionFactory.createConsumer(consumerConfig);
        final AtomicInteger i = new AtomicInteger(0);
        try {
            int count = 2;
            this.sendMessage(count, "hello", this.topic);

            this.consumer.subscribe(this.topic, 1024 * 1024, new MessageListener() {

                public void recieveMessages(final Message messages) {
                    ComsumeFailAndRecoverTest.this.queue.add(messages);
                    // 第一次接收到抛异常,retry 5+1次均抛异常之后消息才进入recover,recover一次处理成功
                    // 共接收这条消息8次
                    // 加上第二条消息,队列里一共有9个消息
                    if (Arrays.equals(messages.getData(), "hello0".getBytes())
                            && i.get() <= consumerConfig.getMaxFetchRetries() + 1) {
                        i.incrementAndGet();
                        throw new RuntimeException("don't worry,just for test");
                    }
                }


                public Executor getExecutor() {
                    return null;
                }
            }).completeSubscribe();

            while (this.queue.size() < count + 2 + consumerConfig.getMaxFetchRetries()) {
                Thread.sleep(1000);
                System.out.println("等待接收消息" + (count + 2 + consumerConfig.getMaxFetchRetries()) + "条，目前接收到"
                        + this.queue.size() + "条");
            }

            int j = 0;
            for (Message msg : this.queue) {
                if (Arrays.equals(msg.getData(), "hello0".getBytes())) {
                    ++j;
                }
                else {
                    System.out.println(new String(msg.getData()));
                    assertTrue(Arrays.equals(msg.getData(), "hello1".getBytes()));
                }
            }
            // 只有一条是hello1,其他都是hello0
            assertEquals(j, this.queue.size() - 1);

        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }
    }
}
