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
package com.taobao.metamorphosis.client.consumer;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Ignore;

import com.taobao.metamorphosis.client.consumer.ConsumerZooKeeper.ZKLoadRebalanceListener;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 协助其他包下的测试类 获取ConsumerZooKeeper包私有的内部状态
 * 
 * @author 无花
 * @since 2011-6-29 下午06:09:33
 */
@Ignore("不参与单元测试")
public class ConsumerZooKeeperAccessor {

    public static ZKLoadRebalanceListener getBrokerConnectionListenerForTest(ConsumerZooKeeper consumerZooKeeper,
            FetchManager fetchManager) {
        return consumerZooKeeper.getBrokerConnectionListener(fetchManager);
    }


    public static Collection<TopicPartitionRegInfo> getTopicPartitionRegInfos(ConsumerZooKeeper consumerZooKeeper,
            FetchManager fetchManager) {
        return getBrokerConnectionListenerForTest(consumerZooKeeper, fetchManager).getTopicPartitionRegInfos();
    }


    public static ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> getTopicRegistry(
            ZKLoadRebalanceListener listener) {
        return listener.topicRegistry;
    }


    public static Set<Broker> getOldBrokerSet(ZKLoadRebalanceListener listener) {
        return listener.oldBrokerSet;
    }

}