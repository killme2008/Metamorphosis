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

import org.apache.commons.lang.StringUtils;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.CommandHeader;
import com.taobao.gecko.core.command.RequestCommand;
import com.taobao.metamorphosis.transaction.TransactionInfo;


/**
 * 事务命令,协议格式如下：</br> transaction transactionKey sessionId type [timeout]
 * [uniqueQualifier] opaque\r\n
 * 
 * 
 * @author boyan
 * 
 */
public class TransactionCommand implements RequestCommand, MetaEncodeCommand {
    private TransactionInfo transactionInfo;
    private final Integer opaque;
    static final long serialVersionUID = -1L;


    @Override
    public Integer getOpaque() {
        return this.opaque;
    }


    @Override
    public CommandHeader getRequestHeader() {
        return this;
    }


    public TransactionCommand(final TransactionInfo transactionInfo, final Integer opaque) {
        super();
        this.transactionInfo = transactionInfo;
        this.opaque = opaque;
    }


    public TransactionInfo getTransactionInfo() {
        return this.transactionInfo;
    }


    public void setTransactionInfo(final TransactionInfo transactionInfo) {
        this.transactionInfo = transactionInfo;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.opaque == null ? 0 : this.opaque.hashCode());
        result = prime * result + (this.transactionInfo == null ? 0 : this.transactionInfo.hashCode());
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
        final TransactionCommand other = (TransactionCommand) obj;
        if (this.opaque == null) {
            if (other.opaque != null) {
                return false;
            }
        }
        else if (!this.opaque.equals(other.opaque)) {
            return false;
        }
        if (this.transactionInfo == null) {
            if (other.transactionInfo != null) {
                return false;
            }
        }
        else if (!this.transactionInfo.equals(other.transactionInfo)) {
            return false;
        }
        return true;
    }


    @Override
    public IoBuffer encode() {
        final String transactionKey = this.transactionInfo.getTransactionId().getTransactionKey();
        final String type = this.transactionInfo.getType().name();
        int capacity =
                17 + transactionKey.length() + this.transactionInfo.getSessionId().length() + type.length()
                + ByteUtils.stringSize(this.opaque);
        if (this.transactionInfo.getTimeout() > 0) {
            capacity += 1 + ByteUtils.stringSize(this.transactionInfo.getTimeout());
        }
        if (!StringUtils.isBlank(this.transactionInfo.getUniqueQualifier())) {
            capacity += 1 + this.transactionInfo.getUniqueQualifier().length();
        }
        final IoBuffer buffer = IoBuffer.allocate(capacity);
        if (this.transactionInfo.getTimeout() > 0) {
            if (StringUtils.isBlank(this.transactionInfo.getUniqueQualifier())) {
                ByteUtils.setArguments(buffer, MetaEncodeCommand.TRANS_CMD, transactionKey,
                    this.transactionInfo.getSessionId(), type, this.transactionInfo.getTimeout(), this.opaque);
            }
            else {
                ByteUtils.setArguments(buffer, MetaEncodeCommand.TRANS_CMD, transactionKey,
                    this.transactionInfo.getSessionId(), type, this.transactionInfo.getTimeout(),
                    this.transactionInfo.getUniqueQualifier(), this.opaque);
            }
        }
        else {
            if (StringUtils.isBlank(this.transactionInfo.getUniqueQualifier())) {
                ByteUtils.setArguments(buffer, MetaEncodeCommand.TRANS_CMD, transactionKey,
                    this.transactionInfo.getSessionId(), type, this.opaque);
            }
            else {
                ByteUtils.setArguments(buffer, MetaEncodeCommand.TRANS_CMD, transactionKey,
                    this.transactionInfo.getSessionId(), type, this.transactionInfo.getUniqueQualifier(), this.opaque);
            }

        }
        buffer.flip();
        return buffer;
    }


    public static long getSerialversionuid() {
        return serialVersionUID;
    }

}