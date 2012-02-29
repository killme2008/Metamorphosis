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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.AbstractRecoverManager;
import com.taobao.metamorphosis.client.consumer.RecoverManager;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Recover存储管理器，需要recover的消息存到notify. <br>
 * <b>unsupported,"notify" is not open source yet<b>
 * 
 * @author 无花
 * @since 2011-10-31 下午3:50:52
 */

public class RecoverNotifyManager extends AbstractRecoverManager {
    private static final Log log = LogFactory.getLog(RecoverNotifyManager.class);


    public RecoverNotifyManager(final MetaMessageSessionFactory factory, final String recoverNotifyTopic,
            final RecoverManager next) {
        super();
    }


    @Override
    public boolean isStarted() {
        return false;
    }


    @Override
    public void start(final MetaClientConfig metaClientConfig) {

    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.client.consumer.RecoverManager#append(java.lang
     * .String, com.taobao.metamorphosis.Message)
     */
    @Override
    public void append(final String group, final Message message) throws IOException {

    }


    @Override
    public void shutdown() throws MetaClientException {
    }

}