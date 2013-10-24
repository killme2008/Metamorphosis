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

import java.util.Arrays;

import com.taobao.gecko.core.buffer.IoBuffer;


/**
 * 同步复制，master/slave复制消息的协议,协议格式如下：</br> sync topic partition value-length flag
 * msgId checksum opaque\r\ndata
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class SyncCommand extends PutCommand {
    private final long msgId;


    public SyncCommand(final String topic, final int partition, final byte[] data, final int flag, final long msgId,
            int checksum, final Integer opaque) {
        super(topic, partition, data, flag, checksum, null, opaque);
        this.msgId = msgId;
    }


    public long getMsgId() {
        return this.msgId;
    }


    @Override
    public IoBuffer encode() {
        final int dataLen = this.data == null ? 0 : this.data.length;
        final IoBuffer buffer =
                IoBuffer.allocate(13 + ByteUtils.stringSize(this.partition) + ByteUtils.stringSize(dataLen)
                    + ByteUtils.stringSize(this.getOpaque()) + this.getTopic().length()
                    + ByteUtils.stringSize(this.msgId) + ByteUtils.stringSize(this.flag)
                    + ByteUtils.stringSize(this.checkSum) + dataLen);

        ByteUtils.setArguments(buffer, MetaEncodeCommand.SYNC_CMD, this.getTopic(), this.partition, dataLen, this.flag,
            this.msgId, this.checkSum, this.getOpaque());

        if (this.data != null) {
            buffer.put(this.data);
        }
        buffer.flip();
        return buffer;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (this.msgId ^ this.msgId >>> 32);
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
        final SyncCommand other = (SyncCommand) obj;
        if (this.msgId != other.msgId) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "SyncCommand [msgId=" + this.msgId + ", data=" + Arrays.toString(this.data) + ", partition="
                + this.partition + ", flag=" + this.flag + ", checkSum=" + this.checkSum + "]";
    }

}