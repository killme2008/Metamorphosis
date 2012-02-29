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
package com.taobao.metamorphosis.client.producer;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 分区选择器
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public interface PartitionSelector {

    /**
     * 根据topic、message从partitions列表中选择分区
     * 
     * @param topic
     *            topic
     * @param partitions
     *            分区列表
     * @param message
     *            消息
     * @return
     * @throws MetaClientException
     *             此方法抛出的任何异常都应当包装为MetaClientException
     */
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
}