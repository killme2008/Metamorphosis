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

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class MetaClientConfig implements Serializable {
    static final long serialVersionUID = -1L;
    protected String serverUrl;

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


    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }


    public void setZkConfig(final ZKConfig zkConfig) {
        if (zkConfig == null) {
            throw new IllegalArgumentException("Null zkconfig");
        }
        if (StringUtils.isEmpty(zkConfig.zkConnect)) {
            throw new IllegalArgumentException("Empty zookeeper servers");
        }
        this.zkConfig = zkConfig;
    }


    public String getZkRoot() {
        return this.zkConfig.getZkRoot();
    }


    public void setZkRoot(String zkRoot) {
        this.zkConfig.setZkRoot(zkRoot);
    }


    public boolean isZkEnable() {
        return this.zkConfig.isZkEnable();
    }


    public void setZkEnable(boolean zkEnable) {
        this.zkConfig.setZkEnable(zkEnable);
    }


    public String getZkConnect() {
        return this.zkConfig.getZkConnect();
    }


    public void setZkConnect(String zkConnect) {
        this.zkConfig.setZkConnect(zkConnect);
    }


    public int getZkSessionTimeoutMs() {
        return this.zkConfig.getZkSessionTimeoutMs();
    }


    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkConfig.setZkSessionTimeoutMs(zkSessionTimeoutMs);
    }


    public int getZkConnectionTimeoutMs() {
        return this.zkConfig.getZkConnectionTimeoutMs();
    }


    public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
        this.zkConfig.setZkConnectionTimeoutMs(zkConnectionTimeoutMs);
    }


    public int getZkSyncTimeMs() {
        return this.zkConfig.getZkSyncTimeMs();
    }


    public void setZkSyncTimeMs(int zkSyncTimeMs) {
        this.zkConfig.setZkSyncTimeMs(zkSyncTimeMs);
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
        return this.partitionsInfo;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.diamondPartitionsDataId == null ? 0 : this.diamondPartitionsDataId.hashCode());
        result = prime * result + (this.diamondPartitionsGroup == null ? 0 : this.diamondPartitionsGroup.hashCode());
        result = prime * result + (this.partitionsInfo == null ? 0 : this.partitionsInfo.hashCode());
        result =
                prime * result + (int) (this.recoverMessageIntervalInMills ^ this.recoverMessageIntervalInMills >>> 32);
        result = prime * result + this.recoverThreadCount;
        result = prime * result + (this.serverUrl == null ? 0 : this.serverUrl.hashCode());
        result = prime * result + (this.zkConfig == null ? 0 : this.zkConfig.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        MetaClientConfig other = (MetaClientConfig) obj;
        if (this.diamondPartitionsDataId == null) {
            if (other.diamondPartitionsDataId != null) {
                return false;
            }
        }
        else if (!this.diamondPartitionsDataId.equals(other.diamondPartitionsDataId)) {
            return false;
        }
        if (this.diamondPartitionsGroup == null) {
            if (other.diamondPartitionsGroup != null) {
                return false;
            }
        }
        else if (!this.diamondPartitionsGroup.equals(other.diamondPartitionsGroup)) {
            return false;
        }
        if (this.partitionsInfo == null) {
            if (other.partitionsInfo != null) {
                return false;
            }
        }
        else if (!this.partitionsInfo.equals(other.partitionsInfo)) {
            return false;
        }
        if (this.recoverMessageIntervalInMills != other.recoverMessageIntervalInMills) {
            return false;
        }
        if (this.recoverThreadCount != other.recoverThreadCount) {
            return false;
        }
        if (this.serverUrl == null) {
            if (other.serverUrl != null) {
                return false;
            }
        }
        else if (!this.serverUrl.equals(other.serverUrl)) {
            return false;
        }
        if (this.zkConfig == null) {
            if (other.zkConfig != null) {
                return false;
            }
        }
        else if (!this.zkConfig.equals(other.zkConfig)) {
            return false;
        }
        return true;
    }

}