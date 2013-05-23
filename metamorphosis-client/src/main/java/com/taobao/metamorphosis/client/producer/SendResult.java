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

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 消息发送结果对象
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
public class SendResult {
    private final boolean success;
    private final Partition partition;
    private final String errorMessage;
    private final long offset;


    public SendResult(boolean success, Partition partition, long offset, String errorMessage) {
        super();
        this.success = success;
        this.partition = partition;
        this.offset = offset;
        this.errorMessage = errorMessage;
    }


    /**
     * 当消息发送成功后，消息在服务端写入的offset，如果发送失败，返回-1
     * 
     * @return
     */
    public long getOffset() {
        return this.offset;
    }


    /**
     * 消息是否发送成功
     * 
     * @return true为成功
     */
    public boolean isSuccess() {
        return this.success;
    }


    /**
     * 消息发送所到达的分区
     * 
     * @return 消息发送所到达的分区，如果发送失败则为null
     */
    public Partition getPartition() {
        return this.partition;
    }


    /**
     * 消息发送结果的附带信息，如果发送失败可能包含错误信息
     * 
     * @return 消息发送结果的附带信息，如果发送失败可能包含错误信息
     */
    public String getErrorMessage() {
        return this.errorMessage;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.errorMessage == null ? 0 : this.errorMessage.hashCode());
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + (this.partition == null ? 0 : this.partition.hashCode());
        result = prime * result + (this.success ? 1231 : 1237);
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
        SendResult other = (SendResult) obj;
        if (this.errorMessage == null) {
            if (other.errorMessage != null) {
                return false;
            }
        }
        else if (!this.errorMessage.equals(other.errorMessage)) {
            return false;
        }
        if (this.offset != other.offset) {
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
        if (this.success != other.success) {
            return false;
        }
        return true;
    }

}