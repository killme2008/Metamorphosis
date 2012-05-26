package com.taobao.meta.test;

import org.junit.Assert;
import org.junit.Test;



/**
 * meta集成测试_topic为空
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class BlankTopicTest extends BaseMetaTest {

    private final String topic = " ";

    @Test(expected = IllegalArgumentException.class)
    public void sendConsume() throws Exception {
        this.createProducer();
        this.producer.publish(this.topic);
        // 订阅者必须指定分组
        this.createConsumer("group1");
        try {
            // 发送消息
            final int count = 5;
            this.sendMessage(count, "hello", this.topic);
            Assert.fail();
            // 订阅接收消息并验证数据正确
            this.subscribe(this.topic, 1024 * 1024, count);

        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Blank topic"));
            e.printStackTrace();
            throw e;
        } finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }

    }
}
