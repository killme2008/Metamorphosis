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

import java.util.List;


/**
 * Consumer的balance策略
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public interface LoadBalanceStrategy {

    enum Type {
        DEFAULT,
        CONSIST
    }


    /**
     * 根据consumer id查找对应的分区列表
     * 
     * @param topic
     *            分区topic
     * @param consumerId
     *            consumerId
     * @param curConsumers
     *            当前所有的consumer列表
     * @param curPartitions
     *            当前的分区列表
     * 
     * @return
     */
    public List<String> getPartitions(String topic, String consumerId, final List<String> curConsumers,
            final List<String> curPartitions);
}