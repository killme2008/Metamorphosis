package com.taobao.meta.test;

import org.junit.Assert;

import org.junit.Test;

import com.taobao.metamorphosis.exception.InvalidConsumerConfigException;

/**
 * meta集成测试_group为空
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class BlankGroupTest extends BaseMetaTest {

	private final String topic = "meta-test";

	@Test
	public void sendConsume() throws Exception {
		createProducer();
		// 需要发布topic
		producer.publish(this.topic);
		try {
			// 订阅者未指定分组
			createConsumer2();
			Assert.fail();

			// 发送消息
			final int count = 5;
			sendMessage(count, "hello", this.topic);

			// 订阅接收消息并验证数据正确
			subscribe(this.topic, 1024 * 1024, count);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof InvalidConsumerConfigException);
			System.out.println("fuck");
			Assert.assertTrue(e.getMessage().indexOf("Blank group") != -1);
		} finally {
			producer.shutdown();
			if (null != consumer) {
				consumer.shutdown();
			}
		}

	}

}
