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

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.client.extension.consumer.ConsumerRecoverType;


/**
 * 一个扩展的Meta会话工厂,提供一些扩展功能.
 * 
 * @author 无花
 * @since 2011-11-7 下午4:06:15
 */

public interface ExtMessageSessionFactory extends BroadcastMessageSessionFactory {

    /**
     * 创建消费者
     * 
     * @param consumerConfig
     *            消费者配置
     * @param RecoverType
     *            选择消息recover方式,
     *            ConsumerRecoverType.DEFAULT,ConsumerRecoverType.NOTIFY<br>
     *            目前不支持ConsumerRecoverType.NOTIFY
     * @return
     */
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig, ConsumerRecoverType RecoverType);


    /**
     * 创建消费者
     * 
     * @param consumerConfig
     *            消费者配置
     * @param offsetStorage
     *            offset存储器
     * @param RecoverType
     *            选择消息recover方式,
     *            ConsumerRecoverType.DEFAULT,ConsumerRecoverType.NOTIFY<br>
     *            目前不支持ConsumerRecoverType.NOTIFY
     * @return
     */
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig, OffsetStorage offsetStorage,
            ConsumerRecoverType recoverType);


    /**
     * 创建广播消费者
     * 
     * @param consumerConfig
     * @param recoverType选择消息recover方式
     *            , ConsumerRecoverType.DEFAULT,ConsumerRecoverType.NOTIFY.<br>
     *            目前不支持ConsumerRecoverType.NOTIFY
     * @return
     */
    public MessageConsumer createBroadcastConsumer(ConsumerConfig consumerConfig, ConsumerRecoverType recoverType);
}