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
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.RecoverManager;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.client.extension.consumer.ConsumerRecoverType;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 一个扩展的Meta会话工厂,提供一些扩展功能.
 * 
 * @author 无花
 * @since 2011-11-7 下午4:09:56
 */

public class ExtMetaMessageSessionFactory extends MetaBroadcastMessageSessionFactory implements
        ExtMessageSessionFactory {

    public ExtMetaMessageSessionFactory(MetaClientConfig metaClientConfig) throws MetaClientException {
        super(metaClientConfig);
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.metamorphosis.client.extension.ExtMessageSessionFactory#
     * createConsumer(com.taobao.metamorphosis.client.consumer.ConsumerConfig,
     * com.taobao.metamorphosis.client.extension.ConsumerRecoverType)
     */
    @Override
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig, ConsumerRecoverType recoverType) {
        return this.createConsumer(consumerConfig, null, recoverType);
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.metamorphosis.client.extension.ExtMessageSessionFactory#
     * createConsumer(com.taobao.metamorphosis.client.consumer.ConsumerConfig,
     * com.taobao.metamorphosis.client.consumer.storage.OffsetStorage,
     * com.taobao.metamorphosis.client.extension.ConsumerRecoverType)
     */
    @Override
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig, OffsetStorage offsetStorage,
            ConsumerRecoverType recoverType) {
        return this.createConsumer(consumerConfig, offsetStorage, this.getRecoverManager(recoverType));
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.metamorphosis.client.extension.ExtMessageSessionFactory#
     * createBroadcastConsumer
     * (com.taobao.metamorphosis.client.consumer.ConsumerConfig,
     * com.taobao.metamorphosis.client.extension.ConsumerRecoverType)
     */
    @Override
    public MessageConsumer createBroadcastConsumer(ConsumerConfig consumerConfig, ConsumerRecoverType recoverType) {
        RecoverManager recoverManager = this.getRecoverManager(recoverType);
        this.addChild(recoverManager);
        return this.createBroadcastConsumer(consumerConfig, recoverManager);
    }


    private RecoverManager getRecoverManager(ConsumerRecoverType recoverType) {
        return recoverType != null ? recoverType.getRecoverManager(this) : null;
    }
}