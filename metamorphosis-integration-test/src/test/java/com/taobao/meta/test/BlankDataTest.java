package com.taobao.meta.test;

import org.junit.Test;

/**
 * meta集成测试_空数据
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class BlankDataTest extends BaseMetaTest {

	private final String topic = "meta-test";

	@Test
	public void sendConsume() throws Exception {
		createProducer();
		// 需要发布topic
		producer.publish(this.topic);
		// 订阅者必须指定分组
		createConsumer("group1");
		try {
			// 发送消息
			final int count = 5;
			sendMessage2(count, "", this.topic);

			// 订阅接收消息并验证数据正确
			subscribe(this.topic, 1024 * 1024, count);
		} finally {
			producer.shutdown();
			consumer.shutdown();
		}
	}
}
