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
package com.taobao.metamorphosis.client;

import java.io.Serializable;
import java.util.Properties;

import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class MetaClientConfig implements Serializable {
    static final long serialVersionUID = -1L;
    protected String serverUrl;

    /**
     * 从diamond获取zk配置的dataId，默认为"metamorphosis.zkConfig"
     */
    protected String diamondZKDataId = DiamondUtils.DEFAULT_ZK_DATAID;

    /**
     * 从diamond获取zk配置的group，默认为DEFAULT_GROUP
     */
    protected String diamondZKGroup = "DEFAULT_GROUP";// Constants.DEFAULT_GROUP;

    /**
     * 从diamond获取partitions配置的dataId，默认为"metamorphosis.partitions"
     * */
    private final String diamondPartitionsDataId = DiamondUtils.DEFAULT_PARTITIONS_DATAID;

    /**
     * 从diamond获取partitions配置的group，默认为DEFAULT_GROUP
     */
    private final String diamondPartitionsGroup = "DEFAULT_GROUP";// Constants.DEFAULT_GROUP;

    protected ZKConfig zkConfig;

    /**
     * recover本地消息的时间间隔
     */
    private long recoverMessageIntervalInMills = 5 * 60 * 1000L;

    private int recoverThreadCount = Runtime.getRuntime().availableProcessors();


    public int getRecoverThreadCount() {
        return this.recoverThreadCount;
    }


    public void setRecoverThreadCount(final int recoverThreadCount) {
        this.recoverThreadCount = recoverThreadCount;
    }


    public long getRecoverMessageIntervalInMills() {
        return this.recoverMessageIntervalInMills;
    }


    public void setRecoverMessageIntervalInMills(final long recoverMessageIntervalInMills) {
        this.recoverMessageIntervalInMills = recoverMessageIntervalInMills;
    }


    public String getDiamondZKDataId() {
        return this.diamondZKDataId;
    }


    public void setDiamondZKDataId(final String diamondZKDataId) {
        this.diamondZKDataId = diamondZKDataId;
    }


    public String getDiamondZKGroup() {
        return this.diamondZKGroup;
    }


    public void setDiamondZKGroup(final String diamondZKGroup) {
        this.diamondZKGroup = diamondZKGroup;
    }


    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }


    public void setZkConfig(final ZKConfig zkConfig) {
        this.zkConfig = zkConfig;
    }


    public String getServerUrl() {
        return this.serverUrl;
    }


    public void setServerUrl(final String serverUrl) {
        this.serverUrl = serverUrl;
    }


    public String getDiamondPartitionsDataId() {
        return this.diamondPartitionsDataId;
    }


    public String getDiamondPartitionsGroup() {
        return this.diamondPartitionsGroup;
    }

    Properties partitionsInfo;


    /**
     * 设置topic的分布情况.
     * 对于使用严格顺序发送消息有效(OrderedMessageProducer),目前版本没有diamond所以从这里设置和获取。 <br>
     * partitionsInfo
     * .put("topic.num.exampleTopic1","brokerId1:分区个数;brokerId2:分区个数...")<br>
     * partitionsInfo
     * .put("topic.num.exampleTopic2","brokerId1:分区个数;brokerId2:分区个数...")
     * 
     * @param partitionsInfo
     */
    public void setPartitionsInfo(final Properties partitionsInfo) {
        this.partitionsInfo = partitionsInfo;
    }


    public Properties getPartitionsInfo() {
        return partitionsInfo;
    }

}