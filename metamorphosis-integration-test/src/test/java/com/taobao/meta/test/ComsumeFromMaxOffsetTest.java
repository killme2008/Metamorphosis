package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;


/**
 * 从实际最大偏移量位置开始接收消息
 * 
 * @author 无花
 * @since 2011-11-14 下午5:03:52
 */

public class ComsumeFromMaxOffsetTest extends BaseMetaTest {
    private final String topic = "meta-test";


    @Test
    public void sendConsume() throws Exception {
        this.createProducer();
        producer.publish(this.topic);

        try {
            // 订阅之前先发送几条消息
            int count = 5;
            this.sendMessage(count, "hello", this.topic);

            Thread.sleep(1000);// 等待服务端刷盘

            ConsumerConfig consumerConfig = new ConsumerConfig("group1");
            consumerConfig.setConsumeFromMaxOffset();// 不接收之前发出的5条
            this.consumer = this.sessionFactory.createConsumer(consumerConfig);

            this.subscribe(this.topic, 1024 * 1024, 0);

            count = 6;
            this.sendMessage(count, "haha", this.topic);// 订阅之后发消息

            this.subscribeRepeatable(this.topic, 1024 * 1024, count);

            // 验证收到的全部都是订阅之后的消息haha
            assertEquals(count, this.queue.size());
            for (Message msg : this.queue) {
                assertTrue(new String(msg.getData()).contains("haha"));
            }
        }
        finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }
}
