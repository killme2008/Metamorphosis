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
package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;


/**
 * 获取消息协议，协议格式如下： get topic group partition offset maxSize opaque\r\n
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public class GetCommand extends AbstractRequestCommand {
    private final long offset;
    private final int maxSize;
    private final int partition;
    private final String group;
    static final long serialVersionUID = -1L;


    public GetCommand(final String topic, final String group, final int partition, final long offset,
            final int maxSize, final Integer opaque) {
        super(topic, opaque);
        this.group = group;
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }


    public String getGroup() {
        return this.group;
    }


    public long getOffset() {
        return this.offset;
    }


    public int getMaxSize() {
        return this.maxSize;
    }


    public int getPartition() {
        return this.partition;
    }


    @Override
    public String toString() {
        return MetaEncodeCommand.GET_CMD + " " + this.getTopic() + " " + this.getGroup() + " " + this.partition + " "
                + this.offset + " " + this.getMaxSize() + " " + this.getOpaque() + "\r\n";
    }


    @Override
    public IoBuffer encode() {
        final IoBuffer buffer =
                IoBuffer.allocate(11 + this.getGroup().length() + ByteUtils.stringSize(this.partition)
                        + ByteUtils.stringSize(this.getOpaque()) + this.getTopic().length()
                        + ByteUtils.stringSize(this.offset) + ByteUtils.stringSize(this.maxSize));
        ByteUtils.setArguments(buffer, MetaEncodeCommand.GET_CMD, this.getTopic(), this.getGroup(), this.partition,
            this.offset, this.maxSize, this.getOpaque());
        buffer.flip();
        return buffer;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (this.group == null ? 0 : this.group.hashCode());
        result = prime * result + this.maxSize;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + this.partition;
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final GetCommand other = (GetCommand) obj;
        if (this.group == null) {
            if (other.group != null) {
                return false;
            }
        }
        else if (!this.group.equals(other.group)) {
            return false;
        }
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.partition != other.partition) {
            return false;
        }
        return true;
    }

}