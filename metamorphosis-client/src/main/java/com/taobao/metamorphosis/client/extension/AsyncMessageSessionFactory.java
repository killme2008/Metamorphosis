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
import com.taobao.metamorphosis.client.extension.producer.AsyncMessageProducer;
import com.taobao.metamorphosis.client.extension.producer.AsyncMessageProducer.IgnoreMessageProcessor;
import com.taobao.metamorphosis.client.producer.PartitionSelector;


/**
 * <pre>
 * 用于创建异步单向发送消息的会话工厂. 
 * 
 * 使用场景: 
 *      对于发送可靠性要求不那么高,但要求提高发送效率和降低对宿主应用的影响，提高宿主应用的稳定性.
 *      例如,收集日志或用户行为信息等场景.
 * 注意:
 *      发送消息后返回的结果中不包含准确的messageId,partition,offset,这些值都是-1
 *      
 * @author 无花
 * @since 2011-10-21 下午2:28:26
 * </pre>
 */

public interface AsyncMessageSessionFactory extends MessageSessionFactory {

    /**
     * 创建异步单向的消息生产者
     * 
     * @return
     */
    public AsyncMessageProducer createAsyncProducer();


    /**
     * 创建异步单向的消息生产者
     * 
     * @param partitionSelector
     *            分区选择器
     * @return
     */
    public AsyncMessageProducer createAsyncProducer(final PartitionSelector partitionSelector);


    /**
     * 创建异步单向的消息生产者
     * 
     * @param partitionSelector
     *            分区选择器
     * @param slidingWindowSize
     *            控制发送流量的滑动窗口大小,4k数据占窗口的一个单位,参考值:窗口大小为20000比较合适. 小于0则用默认值20000.
     *            窗口开得太大可能导致OOM风险
     * @return
     */
    public AsyncMessageProducer createAsyncProducer(final PartitionSelector partitionSelector, int slidingWindowSize);


    /**
     * 创建异步单向的消息生产者
     * 
     * @param partitionSelector
     *            分区选择器
     * @param slidingWindowSize
     *            控制发送流量的滑动窗口大小,4k数据占窗口的一个单位,参考值:窗口大小为20000比较合适. 小于0则用默认值20000.
     *            窗口开得太大可能导致OOM风险
     * @param processor
     *            设置发送失败和超过流控消息的处理器,用户可以自己接管这些消息如何处理
     * 
     * 
     * @return
     */
    public AsyncMessageProducer createAsyncProducer(final PartitionSelector partitionSelector,
            IgnoreMessageProcessor processor);
}