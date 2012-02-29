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
package com.taobao.metamorphosis.client.extension.producer;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * <pre>
 * 支持获取某topic分区总数,当前可用分区数跟配置分区不对应时 将抛出一个特殊的异常
 * <code>AvailablePartitionNumException</code>, 以便发送消息时可识别这个失败原因,从而做相应处理.
 * 
 * 需要按照消息内容(例如某个id)散列到固定分区并要求有序的场景中使用
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-2 下午4:41:35
 */
// 不放在NumAwarePartitionSelector中做是因为,当可用分区跟欲配置的分区信息不一致时,不一定都要按照这一种方式处理,
// 留出扩展余地
public abstract class OrderedMessagePartitionSelector extends ConfigPartitionsSupport {

    @Override
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException {
        int availablePartitionNum = partitions != null ? partitions.size() : 0;
        List<Partition> configPartitions = this.getConfigPartitions(topic);
        int configPartitionsNum = configPartitions.size();

        // 顺序消息没有配置过分区总数信息,不让发消息
        if (configPartitionsNum == 0) {
            throw new MetaClientException("There is no config partitions for topic " + topic
                    + ",maybe you don't config it at first?");
        }

        Partition selectedPartition = this.choosePartition(topic, configPartitions, message);
        if (selectedPartition == null) {
            throw new MetaClientException("selected null partition");
        }

        // 可用分区数为0,对于顺序消息认为是临时没有可用分区(比如只有一台服务器,而它正在重启),不认为是没发布topic,
        // 进入消息写本地机制
        if (availablePartitionNum == 0) {
            throw new AvailablePartitionNumException("selected partition[" + selectedPartition + "]for topic[" + topic
                    + "]can not write now");
        }

        // 可用分区和配置分区均不包含选出来的分区,(可能是用户乱填的,抛出异常用户自己负责)
        if (!configPartitions.contains(selectedPartition) && !partitions.contains(selectedPartition)) {
            throw new MetaClientException("invalid selected partition:" + selectedPartition
                    + ",config and availabe paritions not contains it");
        }

        // 可用分区和配置分区均包含选出来的分区时才可写
        if (configPartitions.contains(selectedPartition) && partitions.contains(selectedPartition)) {
            return selectedPartition;
        }
        else {
            // 选择出来的期望分区不可写.
            // 1.配置分区信息变了
            // 2.可用分区变了(机器暂停或重启)
            throw new AvailablePartitionNumException("selected partition[" + selectedPartition + "]for topic[" + topic
                    + "]can not write now");
        }

    }


    protected abstract Partition choosePartition(String topic, List<Partition> partitions, Message message);

}