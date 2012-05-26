package com.taobao.meta.test;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.exception.InvalidConsumerConfigException;

/**
 * meta集成测试_特殊字符作为GroupName
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */

public class SpecialCharGroupNameTest extends BaseMetaTest {

	private final String topic = "meta-test";

	@Test
	public void sendConsume() throws Exception {
		createProducer();
		producer.publish(this.topic);

		try {
			// 订阅者必须指定分组
			createConsumer("~!@#$%");
			// 发送消息
			final int count = 5;
			sendMessage(count, "hello", this.topic);

			// 订阅接收消息并验证数据正确
			subscribe(this.topic, 1024 * 1024, count);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof InvalidConsumerConfigException);
			Assert.assertTrue(e.getMessage().indexOf(
					"Group name has invalid character") != -1);
		//	e.printStackTrace();
		} finally {
			producer.shutdown();
			if (consumer != null)
				consumer.shutdown();
		}

	}
}
