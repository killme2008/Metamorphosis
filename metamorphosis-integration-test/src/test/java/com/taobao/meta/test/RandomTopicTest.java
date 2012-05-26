package com.taobao.meta.test;

import org.junit.Assert;
import org.junit.Test;

/**
 * meta集成测试_订阅不存在的topic
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

import com.taobao.metamorphosis.exception.MetaClientException;

public class RandomTopicTest extends BaseMetaTest {

	private final String topic = "gongyangyu";

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
			// 订阅接收消息并验证数据正确
			subscribe(this.topic, 1024 * 1024, count);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof MetaClientException);
			Assert.assertTrue(e.getMessage().indexOf("send message failed") != -1);
		} finally {
			producer.shutdown();
			consumer.shutdown();
		}
	}
}
