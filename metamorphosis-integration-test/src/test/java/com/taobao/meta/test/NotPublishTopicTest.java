package com.taobao.meta.test;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.exception.MetaClientException;

/**
 * meta集成测试_未发布topic
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class NotPublishTopicTest extends BaseMetaTest {

    private final String topic = "meta-test";

    @Test
    public void sendConsume() throws Exception {
        this.createProducer();
        // 订阅者必须指定分组
        this.createConsumer("group1");
        try {
            // 发送消息
            final int count = 5;
            this.sendMessage(count, "hello", this.topic);
            Assert.fail();
            // 订阅接收消息并验证数据正确
            this.subscribe(this.topic, 1024 * 1024, count);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MetaClientException);
            Assert.assertTrue(e.getMessage().indexOf("There is no aviable partition for topic") != -1);
        } finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }

    }
}
