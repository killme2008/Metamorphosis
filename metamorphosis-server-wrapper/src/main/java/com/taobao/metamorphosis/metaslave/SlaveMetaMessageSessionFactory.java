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
 *   wuhua <wq163@163.com> 
 */
package com.taobao.metamorphosis.metaslave;

import org.I0Itec.zkclient.ZkClient;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.ConsumerZooKeeper;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-27 ÏÂÎç06:44:48
 */

public class SlaveMetaMessageSessionFactory extends MetaMessageSessionFactory {

    private static int brokerId = -1;


    private SlaveMetaMessageSessionFactory(final MetaClientConfig metaClientConfig) throws MetaClientException {
        super(metaClientConfig);
    }


    public synchronized static SlaveMetaMessageSessionFactory create(final MetaClientConfig metaClientConfig,
            final int brokerId) throws MetaClientException {
        SlaveMetaMessageSessionFactory.brokerId = brokerId;

        return new SlaveMetaMessageSessionFactory(metaClientConfig);
    }


    @Override
    protected ConsumerZooKeeper initConsumerZooKeeper(final RemotingClientWrapper remotingClient,
            final ZkClient zkClient, final ZKConfig zkConfig) {
        if (SlaveMetaMessageSessionFactory.brokerId < 0) {
            throw new IllegalStateException("please set brokerId first");
        }
        return new SlaveConsumerZooKeeper(this.metaZookeeper, remotingClient, zkClient, zkConfig,
            SlaveMetaMessageSessionFactory.brokerId);
    }


    // for test
    static int getBrokerId() {
        return SlaveMetaMessageSessionFactory.brokerId;
    }

}