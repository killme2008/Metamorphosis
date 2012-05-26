package com.taobao.meta.test;

import org.junit.Test;

/**
 * meta集成测试_SubscribeWrongTopicTest
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class SubscribeWrongTopicTest extends BaseMetaTest {

	private final String topic = "meta-test";

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
			subscribe("gongyangyu", 1024*1024, 0);
		} finally {
			producer.shutdown();
			consumer.shutdown();
		}
	}
}
