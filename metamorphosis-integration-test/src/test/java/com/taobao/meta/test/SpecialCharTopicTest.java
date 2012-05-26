package com.taobao.meta.test;

import org.junit.Assert;
import org.junit.Test;

/**
 * meta集成测试_特殊字符作为topic
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class SpecialCharTopicTest extends BaseMetaTest {

    private final String topic = "!@#$%";

    @Test
    public void sendConsume() throws Exception {
    	createProducer();
		producer.publish(this.topic);
		// 订阅者必须指定分组
		createConsumer("group1");

		try {
			// 发送消息
			final int count = 5;
			sendMessage(count, "hello", this.topic);
            Assert.fail();
			// 订阅接收消息并验证数据正确
			subscribe(this.topic, 1024 * 1024, count);
		}
        catch(Exception e)
        {
        	Assert.assertTrue(e instanceof RuntimeException);
        	Assert.assertTrue(e.getMessage().indexOf("The server do not accept topic !@#$%") != -1);
        }
        finally {
            producer.shutdown();
            consumer.shutdown();
        }

    }
}
