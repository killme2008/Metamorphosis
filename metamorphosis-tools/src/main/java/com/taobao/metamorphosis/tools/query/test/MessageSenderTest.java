/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.tools.query.test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MessageSenderTest {
	public static void main(String[] args) throws Exception {
		MetaClientConfig config = new MetaClientConfig();
		ZKConfig zkConfig = new ZKConfig("10.249.197.121", 30000, 30000, 5000);
		config.setZkConfig(zkConfig);
		MetaMessageSessionFactory factory = new MetaMessageSessionFactory(config);
		MessageProducer producer = factory.createProducer(false);
		String topic = "test";
		producer.publish(topic);
		Message message = new Message(topic, new byte[128]);
		producer.sendMessage(message);
		
	}
	
}