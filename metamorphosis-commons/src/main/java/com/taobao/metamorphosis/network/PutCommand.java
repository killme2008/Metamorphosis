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
import com.taobao.gecko.core.command.CommandHeader;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 发送消息命令，协议格式： </br></br> put topic partition value-length flag
 * [transactionkey] opaque\r\n data </br></br> data的结构如下：</br> </br>
 * attribute(0个或者1个，固定长度字符串，取决于flag字段) + binary data
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public class PutCommand extends AbstractRequestCommand {
    static final long serialVersionUID = -1L;
    protected byte[] data;
    protected int partition;
    protected final int flag;
    private TransactionId transactionId;


    public TransactionId getTransactionId() {
        return this.transactionId;
    }


    public void setTransactionId(final TransactionId transactionId) {
        this.transactionId = transactionId;
    }


    public byte[] getData() {
        return this.data;
    }


    public int getFlag() {
        return this.flag;
    }


    public void setData(final byte[] data) {
        this.data = data;
    }


    public void setPartition(final int partition) {
        this.partition = partition;
    }


    public int getPartition() {
        return this.partition;
    }


    public PutCommand(final String topic, final int partition, final byte[] data, final TransactionId transactionId,
            final int flag, final Integer opaque) {
        super(topic, opaque);
        this.partition = partition;
        this.data = data;
        this.flag = flag;
        this.transactionId = transactionId;
    }


    @Override
    public CommandHeader getRequestHeader() {
        return new CommandHeader() {
            @Override
            public Integer getOpaque() {
                return PutCommand.this.getOpaque();
            }
        };
    }


    @Override
    public IoBuffer encode() {
        final int dataLen = this.data == null ? 0 : this.data.length;
        final String transactionKey = this.transactionId != null ? this.transactionId.getTransactionKey() : null;

        final IoBuffer buffer =
                IoBuffer.allocate(10 + ByteUtils.stringSize(this.partition) + ByteUtils.stringSize(dataLen)
                        + ByteUtils.stringSize(this.getOpaque()) + this.getTopic().length()
                        + (transactionKey != null ? transactionKey.length() + 1 : 0) + ByteUtils.stringSize(this.flag)
                        + dataLen);
        if (transactionKey != null) {
            ByteUtils.setArguments(buffer, MetaEncodeCommand.PUT_CMD, this.getTopic(), this.partition, dataLen,
                this.flag, transactionKey, this.getOpaque());
        }
        else {
            ByteUtils.setArguments(buffer, MetaEncodeCommand.PUT_CMD, this.getTopic(), this.partition, dataLen,
                this.flag, this.getOpaque());
        }
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
        result = prime * result + Arrays.hashCode(this.data);
        result = prime * result + this.flag;
        result = prime * result + this.partition;
        result = prime * result + (this.transactionId == null ? 0 : this.transactionId.hashCode());
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
        final PutCommand other = (PutCommand) obj;
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        if (this.flag != other.flag) {
            return false;
        }
        if (this.partition != other.partition) {
            return false;
        }
        if (this.transactionId == null) {
            if (other.transactionId != null) {
                return false;
            }
        }
        else if (!this.transactionId.equals(other.transactionId)) {
            return false;
        }
        return true;
    }

}