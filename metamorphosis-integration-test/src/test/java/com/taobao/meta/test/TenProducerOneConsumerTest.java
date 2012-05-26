package com.taobao.meta.test;

import org.junit.Test;

/**
 * meta集成测试_TenProducerOneConsumerTest
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class TenProducerOneConsumerTest extends BaseMetaTest {

	private final String topic = "meta-test";

	@Test
	public void sendConsume() throws Exception {

		create_nProducer(10);
		// 需要发布topic
		// 订阅者必须指定分组
		createConsumer("group1");

		try {
			// 发送消息
			final int count = 5;
			sendMessage_nProducer(count, "hello", this.topic, 10);
			// 订阅接收消息并验证数据正确
			subscribe(this.topic, 1024 * 1024, 50);
		} finally {
			for (int i = 0; i < 10; i++) {
				producerList.get(i).shutdown();
			}
			consumer.shutdown();
		}

	}
}
