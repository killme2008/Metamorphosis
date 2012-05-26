package com.taobao.meta.test;

import org.junit.Test;



public class OneProducerTenConsumerTenGroupTest extends BaseMetaTest {

	private final String topic = "meta-test";

	@Test
	public void sendConsume() throws Exception {
		createProducer();
		// 需要发布topic
		producer.publish(this.topic);
		// 订阅者必须指定分组

		try {
			// 发送消息
			final int count = 50;
			sendMessage(count, "hello", this.topic);
			// 订阅接收消息并验证数据正确
			subscribe_nConsumer(topic, 1024 * 1024, count, 10,1);
		} finally {
			producer.shutdown();
			for (int i = 0; i < 10; i++) {
				consumerList.get(i).shutdown();
			}
		}
	}
}
