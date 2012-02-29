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

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.producer.AsyncMessageProducer;
import com.taobao.metamorphosis.client.extension.producer.AsyncMessageProducer.IgnoreMessageProcessor;
import com.taobao.metamorphosis.client.extension.producer.AsyncMetaMessageProducer;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.RoundRobinPartitionSelector;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 用于创建异步单向发送消息的会话工厂.
 * 
 * @author 无花
 * @since 2011-10-21 下午2:29:55
 */

public class AsyncMetaMessageSessionFactory extends MetaMessageSessionFactory implements AsyncMessageSessionFactory {

    public AsyncMetaMessageSessionFactory(final MetaClientConfig metaClientConfig) throws MetaClientException {
        super(metaClientConfig);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.client.extension.AsyncMessageSessionFactory#
     * createAsyncProducer()
     */
    @Override
    public AsyncMessageProducer createAsyncProducer() {
        return this.createAsyncProducer(new RoundRobinPartitionSelector());
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.client.extension.AsyncMessageSessionFactory#
     * createAsyncProducer
     * (com.taobao.metamorphosis.client.producer.PartitionSelector)
     */
    @Override
    public AsyncMessageProducer createAsyncProducer(PartitionSelector partitionSelector) {
        return this.createAsyncProducer(partitionSelector, 0);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.client.extension.AsyncMessageSessionFactory#
     * createAsyncProducer
     * (com.taobao.metamorphosis.client.producer.PartitionSelector, int)
     */
    @Override
    public AsyncMessageProducer createAsyncProducer(PartitionSelector partitionSelector, int slidingWindowSize) {
        return this.createAsyncProducer(partitionSelector, slidingWindowSize, null);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.client.extension.AsyncMessageSessionFactory#
     * createAsyncProducer
     * (com.taobao.metamorphosis.client.producer.PartitionSelector,
     * com.taobao.metamorphosis
     * .client.extension.producer.AsyncMessageProducer.IgnoreMessageProcessor)
     */
    @Override
    public AsyncMessageProducer createAsyncProducer(PartitionSelector partitionSelector,
            IgnoreMessageProcessor processor) {
        return this.createAsyncProducer(partitionSelector, 0, processor);
    }


    private AsyncMessageProducer createAsyncProducer(PartitionSelector partitionSelector, int slidingWindowSize,
            IgnoreMessageProcessor processor) {
        return new AsyncMetaMessageProducer(this, this.remotingClient, partitionSelector, this.producerZooKeeper,
            this.sessionIdGenerator.generateId(), slidingWindowSize, processor);
    }

}