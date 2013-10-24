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
package com.taobao.metamorphosis;

import java.io.Serializable;
import java.util.Arrays;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * A message with topic and data,a string attribute is optional.
 * 
 * @author boyan
 * @Date 2011-4-19
 * @author wuhua
 * @Date 2011-6-26
 * 
 */
public class Message implements Serializable {
    static final long serialVersionUID = -1L;
    private long id;
    private String topic;
    private byte[] data;
    private String attribute;
    private int flag;
    private Partition partition;
    // added by dennis<killme2008@gmail.com>,2012-06-14
    private transient boolean readOnly;

    private transient boolean rollbackOnly = false;


    void setId(final long id) {
        this.id = id;
    }


    private void checkState() {
        if (this.readOnly) {
            throw new IllegalStateException("The message is readonly");
        }
    }


    int getFlag() {
        return this.flag;
    }


    void setFlag(final int flag) {
        this.flag = flag;
    }


    boolean isRollbackOnly() {
        return this.rollbackOnly;
    }


    /**
     * Set message to be in rollback only state.The state is transient,it's only
     * valid in current message instance.
     * 
     * @since 1.4.5
     */
    public void setRollbackOnly() {
        this.rollbackOnly = true;
    }


    /**
     * Returns whether the message is readonly.
     * 
     * @since 1.4.4
     * @return
     */
    public boolean isReadOnly() {
        return this.readOnly;
    }


    /**
     * Set the message to be readonly,but metamorphosis client and server could
     * modify message's id,flag,partition.The readonly state is transient,it
     * will not be persist in broker.
     * 
     * @since 1.4.4
     * @param readOnly
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }


    /**
     * Returns whether the message has an attribute.
     * 
     * @return
     */
    public boolean hasAttribute() {
        return this.attribute != null;
    }


    /**
     * Returns whether the message is in order,it is deprecated and will be
     * removed in future version.
     * 
     * @return
     */
    @Deprecated
    public boolean isOrdered() {
        return false;
    }


    public Message(final String topic, final byte[] data) {
        super();
        this.topic = topic;
        this.data = data;
    }


    public Message(final String topic, final byte[] data, final String attribute) {
        super();
        this.topic = topic;
        this.data = data;
        this.attribute = attribute;
    }


    /**
     * Returns the message's id.If it was sent success,the message id would be
     * returned by broker,otherwise is zero.
     * 
     * @return
     */
    public long getId() {
        return this.id;
    }


    /**
     * Returns the message's attribute,may be null.
     * 
     * @return
     */
    public String getAttribute() {
        return this.attribute;
    }


    /**
     * Set message's attribute
     * 
     * @param attribute
     */
    public void setAttribute(final String attribute) {
        this.checkState();
        this.attribute = attribute;
    }


    /**
     * Set message's topic,if you want to send it,you must publish it at first
     * with MessageProducer.
     * 
     * @see com.taobao.metamorphosis.client.producer.MessageProducer#publish(String)
     * @param topic
     */
    public void setTopic(final String topic) {
        this.checkState();
        this.topic = topic;
    }


    /**
     * Set the message's payload
     * 
     * @param data
     */
    public void setData(final byte[] data) {
        this.checkState();
        this.data = data;
    }


    /**
     * Returns message's topic
     * 
     * @return
     */
    public String getTopic() {
        return this.topic;
    }


    /**
     * Returns message's payload
     * 
     * @return
     */
    public byte[] getData() {
        return this.data;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.attribute == null ? 0 : this.attribute.hashCode());
        result = prime * result + Arrays.hashCode(this.data);
        result = prime * result + (int) (this.id ^ this.id >>> 32);
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
        final Message other = (Message) obj;
        if (this.attribute == null) {
            if (other.attribute != null) {
                return false;
            }
        }
        else if (!this.attribute.equals(other.attribute)) {
            return false;
        }
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        if (this.id != other.id) {
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


    void setPartition(final Partition partition) {
        this.partition = partition;
    }


    /**
     * Returns message's partition in broker,if it was sent fail,it would be
     * null.
     * 
     * @return
     */
    public Partition getPartition() {
        return this.partition;
    }

}