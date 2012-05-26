package com.taobao.meta.test;

import org.junit.Test;

/**
 * meta集成测试_TenProducerTenConsumerTenGroupTwoTopic
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class TenProducerTenConsumerTenGroupTwoTopicTest extends BaseMetaTest {

	private final String topic1 = "meta-test";
	private final String topic2 = "meta-test2";

	@Test
	public void sendConsume() throws Exception {

		create_nProducer(10);

		try {
			// 发送消息
			final int count = 5;
			sendMessage_nProducer_twoTopic(count, "hello", this.topic1,
					this.topic2, 10, false,"langneng","gongyangyu");

			// 订阅接收消息
			subscribe_nConsumer_twoTopic(this.topic1, this.topic2, 1024 * 1024,
					10, 10, 10);
		} finally {
			for (int i = 0; i < 10; i++) {
				producerList.get(i).shutdown();
				consumerList.get(i).shutdown();
			}
		}
	}
}