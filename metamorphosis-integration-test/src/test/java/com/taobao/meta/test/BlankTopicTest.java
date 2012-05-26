package com.taobao.meta.test;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.common.lang.StringUtil;

/**
 * meta集成测试_topic为空
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class BlankTopicTest extends BaseMetaTest {

	private final String topic = " ";

	@Test(expected = IllegalArgumentException.class)
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

		} catch (IllegalArgumentException e) {
			Assert.assertTrue(StringUtil.contains(e.getMessage(), "Blank topic"));
			e.printStackTrace();
			throw e;
		} finally {
			producer.shutdown();
			consumer.shutdown();
		}

	}
}
