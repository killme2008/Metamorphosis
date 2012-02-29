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
 * 基于Tair的offset保存期
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
// TODO
public class TairOffsetStorage implements OffsetStorage {

    public void commitOffset(String group, Collection<TopicPartitionRegInfo> infoList) {
        // TODO Auto-generated method stub

    }


    public TopicPartitionRegInfo load(String topic, String group, Partition partition) {
        // TODO Auto-generated method stub
        return null;
    }


    public void close() {
        // TODO Auto-generated method stub

    }


    public void initOffset(String topic, String group, Partition partition, long offset) {
        // TODO Auto-generated method stub

    }

}