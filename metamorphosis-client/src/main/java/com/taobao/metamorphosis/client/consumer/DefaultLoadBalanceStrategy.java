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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 默认的负载均衡策略，尽量使得负载在所有consumer之间平均分配，consumer之间分配的分区数差距不大于1
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public class DefaultLoadBalanceStrategy implements LoadBalanceStrategy {

    static final Log log = LogFactory.getLog(DefaultLoadBalanceStrategy.class);


    @Override
    public List<String> getPartitions(final String topic, final String consumerId, final List<String> curConsumers,
            final List<String> curPartitions) {
        // 每个订阅者平均挂载的partition数目
        final int nPartsPerConsumer = curPartitions.size() / curConsumers.size();
        // 挂载到额外partition的consumer数目
        final int nConsumersWithExtraPart = curPartitions.size() % curConsumers.size();

        log.info("Consumer " + consumerId + " rebalancing the following partitions: " + curPartitions + " for topic "
                + topic + " with consumers: " + curConsumers);
        final int myConsumerPosition = curConsumers.indexOf(consumerId);
        if (myConsumerPosition < 0) {
            log.warn("No broker partions consumed by consumer " + consumerId + " for topic " + topic);
            return Collections.emptyList();
        }
        assert myConsumerPosition >= 0;
        // 计算起点
        final int startPart =
                nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition, nConsumersWithExtraPart);
        final int nParts = nPartsPerConsumer + (myConsumerPosition + 1 > nConsumersWithExtraPart ? 0 : 1);

        if (nParts <= 0) {
            log.warn("No broker partions consumed by consumer " + consumerId + " for topic " + topic);
            return Collections.emptyList();
        }
        final List<String> rt = new ArrayList<String>();
        for (int i = startPart; i < startPart + nParts; i++) {
            final String partition = curPartitions.get(i);
            rt.add(partition);
        }
        return rt;
    }

}