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
package com.taobao.metamorphosis.client;

import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;


/**
 * 用于创建XA消息会话的工厂
 * 
 * @author boyan
 * 
 */
public interface XAMessageSessionFactory extends MessageSessionFactory {

    /**
     * 创建XA消息生产者
     * 
     * @param partitionSelector
     *            分区选择器
     * @return
     */
    public XAMessageProducer createXAProducer(PartitionSelector partitionSelector);


    /**
     * 创建XA消息生产者，默认使用轮询分区选择器
     * 
     * @return
     */
    public XAMessageProducer createXAProducer();

}