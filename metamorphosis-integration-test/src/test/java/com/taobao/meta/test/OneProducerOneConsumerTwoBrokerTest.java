package com.taobao.meta.test;

import org.junit.Test;

/**
 * meta集成测试_OneProducerOneConsumerTwoBroker
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class OneProducerOneConsumerTwoBrokerTest extends BaseMetaTest {

	private final String topic = "meta-test";

	@Test
	public void sendConsume() throws Exception {
		this.startServer("server2");
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
		} finally {
			producer.shutdown();
			consumer.shutdown();
		}
	}
}
