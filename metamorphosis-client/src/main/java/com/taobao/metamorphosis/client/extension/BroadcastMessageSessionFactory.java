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
package com.taobao.metamorphosis.client.extension;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;


/**
 * 广播消息会话工厂,使用这个创建的Consumer在同一分组内的每台机器都能收到同一条消息,
 * 推荐一个应用只使用一个MessageSessionFactory
 * 
 * @author 无花
 * @since 2011-6-13 下午02:49:27
 */

public interface BroadcastMessageSessionFactory extends MessageSessionFactory {

    /**
     * 创建广播方式接收的消息消费者，offset将存储在本地
     * 
     * @param consumerConfig
     *            消费者配置
     * 
     * @return
     * */
    public MessageConsumer createBroadcastConsumer(ConsumerConfig consumerConfig);
}