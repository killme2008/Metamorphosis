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
package com.taobao.metamorphosis.client.consumer.storage;

import java.util.Collection;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * Offset存储器接口
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
public interface OffsetStorage {
    /**
     * 保存offset到存储
     * 
     * @param group
     *            消费者组名
     * @param infoList
     *            消费者订阅的消息分区信息列表
     */
    public void commitOffset(String group, Collection<TopicPartitionRegInfo> infoList);


    /**
     * 加载一条消费者的订阅信息，如果不存在返回null
     * 
     * @param topic
     * @param group
     * @param partiton
     * @return
     */
    public TopicPartitionRegInfo load(String topic, String group, Partition partition);


    /**
     * 释放资源，meta客户端在关闭的时候会主动调用此方法
     */
    public void close();


    /**
     * 初始化offset
     * 
     * @param topic
     * @param group
     * @param partition
     * @param offset
     */
    public void initOffset(String topic, String group, Partition partition, long offset);
}