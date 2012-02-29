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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.DiamondUtils;


/**
 * 负责从diamond取得分区的配置信息（针对顺序消息的分区预配置）
 * 
 * @author 无花
 * @since 2011-8-17 下午3:32:58
 */

public class ProducerDiamondManager {

    private static Log log = LogFactory.getLog(ProducerDiamondManager.class);

    // private final DiamondManager partitionsDiamondManager;

    private final Map<String/* topic */, List<Partition>> partitionsMap =
            new ConcurrentHashMap<String, List<Partition>>();


    public ProducerDiamondManager(final MetaClientConfig metaClientConfig) {
        // this.partitionsDiamondManager =
        // new
        // DefaultDiamondManager(metaClientConfig.getDiamondPartitionsGroup(),
        // metaClientConfig.getDiamondPartitionsDataId(), new ManagerListener()
        // {
        //
        // @Override
        // public Executor getExecutor() {
        // return null;
        // }
        //
        //
        // @Override
        // public void receiveConfigInfo(String configInfo) {
        // final Properties properties = new Properties();
        // try {
        // properties.load(new StringReader(configInfo));
        // DiamondUtils.getPartitions(properties,
        // ProducerDiamondManager.this.partitionsMap);
        // }
        // catch (Exception e) {
        // log.error("从diamond加载zk配置失败", e);
        // }
        // }
        //
        // });
        DiamondUtils.getPartitions(metaClientConfig.getPartitionsInfo(), this.partitionsMap);
    }


    // for test
    // ProducerDiamondManager(final DiamondManager partitionsDiamondManager) {
    // this.partitionsDiamondManager = partitionsDiamondManager;
    // DiamondUtils.getPartitions(this.partitionsDiamondManager, 10000,
    // this.partitionsMap);
    // }

    public Map<String, List<Partition>> getPartitions() {
        return Collections.unmodifiableMap(this.partitionsMap);
    }
}