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
import java.util.Map;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 支持获取某topic预配置的分区分布情况
 * 
 * @author 无花
 * @since 2011-8-2 下午02:49:27
 */
public interface ConfigPartitionsAware {

    /**
     * 设置顺序消息配置的总体分区信息
     * */
    public void setConfigPartitions(Map<String/* topic */, List<Partition>/* partitions */> map);


    /**
     * 获取某个topic消息的总体分区信息
     * */
    public List<Partition> getConfigPartitions(String topic);
}