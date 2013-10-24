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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.MessageIterator;


/**
 * 订阅消息的注册信息
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public class TopicPartitionRegInfo implements Serializable {
    static final long serialVersionUID = -1L;
    private String topic;
    private Partition partition;
    private final AtomicLong offset;
    // 存储上一次消费的messageId,为了同步复制功能实现
    // added by boyan
    private long messageId = -1L;

    private boolean modified;


    public TopicPartitionRegInfo clone(MessageIterator it) {
        return new TopicPartitionRegInfo(this.topic, this.partition, this.offset.get() + it.getOffset(), this.messageId);
    }

    public synchronized boolean isModified() {
        return this.modified;
    }


    public synchronized void setModified(final boolean modified) {
        this.modified = modified;
    }


    public synchronized long getMessageId() {
        return this.messageId;
    }


    public synchronized void setMessageId(final long messageId) {
        this.messageId = messageId;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.messageId ^ this.messageId >>> 32);
        final long currOffset = this.offset.get();
        result = prime * result + (int) (currOffset ^ currOffset >>> 32);
        result = prime * result + (this.partition == null ? 0 : this.partition.hashCode());
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final TopicPartitionRegInfo other = (TopicPartitionRegInfo) obj;
        if (this.messageId != other.messageId) {
            return false;
        }
        if (this.offset == null) {
            if (other.offset != null) {
                return false;
            }
        }
        else if (this.offset.get() != other.offset.get()) {
            return false;
        }
        if (this.partition == null) {
            if (other.partition != null) {
                return false;
            }
        }
        else if (!this.partition.equals(other.partition)) {
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
        return true;
    }


    public TopicPartitionRegInfo(final String topic, final Partition partition, final long offset) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.offset = new AtomicLong(offset);
    }


    public TopicPartitionRegInfo(final String topic, final Partition partition, final long offset, final long messageId) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.offset = new AtomicLong(offset);
        this.messageId = messageId;
    }


    public String getTopic() {
        return this.topic;
    }


    public void setTopic(final String topic) {
        this.topic = topic;
    }


    public Partition getPartition() {
        return this.partition;
    }


    public void setPartition(final Partition partition) {
        this.partition = partition;
    }


    public synchronized AtomicLong getOffset() {
        return this.offset;
    }

}