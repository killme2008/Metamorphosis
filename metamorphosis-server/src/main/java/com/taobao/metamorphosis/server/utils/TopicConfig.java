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
package com.taobao.metamorphosis.server.utils;

import java.util.HashMap;
import java.util.Map;

import com.taobao.metamorphosis.utils.Config;


/**
 * 针对某个topic的特殊配置（不使用全局配置）
 * 
 * @author 无花,dennis
 * @since 2011-8-18 下午2:30:35
 */
// TODO 将其他针对某个topic的特殊配置项移到这里
public class TopicConfig extends Config {
    private String topic;
    private int unflushThreshold;
    private int unflushInterval;
    private String dataPath;
    private String deleteWhen;
    private String deletePolicy;
    private int numPartitions;
    private boolean acceptPublish = true;
    private boolean acceptSubscribe = true;
    private boolean stat;
    private Map<String/* group name */, String/* class name */> filterClassNames = new HashMap<String, String>();


    public final void addFilterClass(String group, String className) {
        this.filterClassNames.put(group, className);
    }


    public final String getFilterClass(String group) {
        return this.filterClassNames.get(group);
    }


    public TopicConfig(final String topic, final MetaConfig metaConfig) {
        this.topic = topic;
        this.unflushThreshold = metaConfig.getUnflushThreshold();
        this.unflushInterval = metaConfig.getUnflushInterval();
        this.dataPath = metaConfig.getDataPath();
        this.deleteWhen = metaConfig.getDeleteWhen();
        this.deletePolicy = metaConfig.getDeletePolicy();
        this.numPartitions = metaConfig.getNumPartitions();
        this.acceptPublish = metaConfig.isAcceptPublish();
        this.acceptSubscribe = metaConfig.isAcceptSubscribe();
        this.stat = metaConfig.isStat();
    }


    public TopicConfig(String topic, int unflushThreshold, int unflushInterval, String dataPath, String deleteWhen,
            String deletePolicy, int numPartitions, boolean acceptPublish, boolean acceptSubscribe, boolean stat,
            Map<String/* group name */, String/* class name */> filterClassNames) {
        super();
        this.topic = topic;
        this.unflushThreshold = unflushThreshold;
        this.unflushInterval = unflushInterval;
        this.dataPath = dataPath;
        this.deleteWhen = deleteWhen;
        this.deletePolicy = deletePolicy;
        this.numPartitions = numPartitions;
        this.acceptPublish = acceptPublish;
        this.acceptSubscribe = acceptSubscribe;
        this.stat = stat;
        this.filterClassNames = filterClassNames;
    }


    @Override
    public TopicConfig clone() {
        return new TopicConfig(this.topic, this.unflushThreshold, this.unflushInterval, this.dataPath, this.deleteWhen,
            this.deletePolicy, this.numPartitions, this.acceptPublish, this.acceptSubscribe, this.stat,
            this.filterClassNames);
    }


    public boolean isAcceptPublish() {
        return this.acceptPublish;
    }


    public void setAcceptPublish(boolean acceptPublish) {
        this.acceptPublish = acceptPublish;
    }


    public boolean isAcceptSubscribe() {
        return this.acceptSubscribe;
    }


    public void setAcceptSubscribe(boolean acceptSubscribe) {
        this.acceptSubscribe = acceptSubscribe;
    }


    public int getNumPartitions() {
        return this.numPartitions;
    }


    public void setNumPartitions(final int numPartitions) {
        this.numPartitions = numPartitions;
    }


    public String getDeletePolicy() {
        return this.deletePolicy;
    }


    public boolean isStat() {
        return this.stat;
    }


    public void setStat(boolean stat) {
        this.stat = stat;
    }


    public void setDeletePolicy(final String deletePolicy) {
        this.deletePolicy = deletePolicy;
    }


    public String getDeleteWhen() {
        return this.deleteWhen;
    }


    public void setDeleteWhen(final String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }


    public String getDataPath() {
        return this.dataPath;
    }


    public void setDataPath(final String dataPath) {
        this.dataPath = dataPath;
    }


    public String getTopic() {
        return this.topic;
    }


    public void setTopic(final String topic) {
        this.topic = topic;
    }


    public int getUnflushThreshold() {
        return this.unflushThreshold;
    }


    public void setUnflushThreshold(final int unflushThreshold) {
        this.unflushThreshold = unflushThreshold;
    }


    public int getUnflushInterval() {
        return this.unflushInterval;
    }


    public void setUnflushInterval(final int unflushInterval) {
        this.unflushInterval = unflushInterval;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.acceptPublish ? 1231 : 1237);
        result = prime * result + (this.acceptSubscribe ? 1231 : 1237);
        result = prime * result + (this.dataPath == null ? 0 : this.dataPath.hashCode());
        result = prime * result + (this.deletePolicy == null ? 0 : this.deletePolicy.hashCode());
        result = prime * result + (this.deleteWhen == null ? 0 : this.deleteWhen.hashCode());
        result = prime * result + (this.filterClassNames == null ? 0 : this.filterClassNames.hashCode());
        result = prime * result + this.numPartitions;
        result = prime * result + (this.stat ? 1231 : 1237);
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
        result = prime * result + this.unflushInterval;
        result = prime * result + this.unflushThreshold;
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
        TopicConfig other = (TopicConfig) obj;
        if (this.acceptPublish != other.acceptPublish) {
            return false;
        }
        if (this.acceptSubscribe != other.acceptSubscribe) {
            return false;
        }
        if (this.dataPath == null) {
            if (other.dataPath != null) {
                return false;
            }
        }
        else if (!this.dataPath.equals(other.dataPath)) {
            return false;
        }
        if (this.deletePolicy == null) {
            if (other.deletePolicy != null) {
                return false;
            }
        }
        else if (!this.deletePolicy.equals(other.deletePolicy)) {
            return false;
        }
        if (this.deleteWhen == null) {
            if (other.deleteWhen != null) {
                return false;
            }
        }
        else if (!this.deleteWhen.equals(other.deleteWhen)) {
            return false;
        }
        if (this.filterClassNames == null) {
            if (other.filterClassNames != null) {
                return false;
            }
        }
        else if (!this.filterClassNames.equals(other.filterClassNames)) {
            return false;
        }
        if (this.numPartitions != other.numPartitions) {
            return false;
        }
        if (this.stat != other.stat) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        }
        else if (!this.topic.equals(other.topic)) {
            return false;
        }
        if (this.unflushInterval != other.unflushInterval) {
            return false;
        }
        if (this.unflushThreshold != other.unflushThreshold) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "TopicConfig [topic=" + this.topic + ", unflushThreshold=" + this.unflushThreshold
                + ", unflushInterval=" + this.unflushInterval + ", dataPath=" + this.dataPath + ", deleteWhen="
                + this.deleteWhen + ", deletePolicy=" + this.deletePolicy + ", numPartitions=" + this.numPartitions
                + ", acceptPublish=" + this.acceptPublish + ", acceptSubscribe=" + this.acceptSubscribe + ", stat="
                + this.stat + ", filterClassNames=" + this.filterClassNames + "]";
    }

}