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
package com.taobao.metamorphosis.client.extension.consumer;

import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.RecoverManager;


/**
 * 消费者消息处理失败时的recover方式
 * 
 * @author 无花
 * @since 2011-11-7 下午5:20:06
 */

public enum ConsumerRecoverType {

    /**
     * 默认类型,需要recover的消息存储在本地
     * 
     */
    DEFAULT {
        @Override
        public RecoverManager getRecoverManager(final MetaMessageSessionFactory factory) {
            return factory.getRecoverStorageManager();
        }
    },

    /**
     * 需要recover的消息存储在Notify.<br>
     * <b>unsupported,"notify" is not open source yet<b>
     * 
     */
    NOTIFY {
        @Override
        public RecoverManager getRecoverManager(final MetaMessageSessionFactory factory) {
            return this.getNotifyRecoverManager(factory, null);
        }

    };

    public RecoverManager getRecoverManager(final MetaMessageSessionFactory factory) {
        throw new AbstractMethodError();
    }


    public RecoverManager getNotifyRecoverManager(final MetaMessageSessionFactory factory,
            final String notifyRecoverTopic) {
        return new RecoverNotifyManager(factory, notifyRecoverTopic, factory.getRecoverStorageManager());
    }
}