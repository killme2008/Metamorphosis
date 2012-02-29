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
import com.taobao.gecko.core.command.ResponseStatus;
import com.taobao.gecko.core.command.kernel.BooleanAckCommand;


/**
 * 应答命令,协议格式如下：</br> result code length opaque\r\n message
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public class BooleanCommand extends AbstractResponseCommand implements BooleanAckCommand {

    private String message;

    /**
     * status code in http protocol
     */
    private final int code;
    static final long serialVersionUID = -1L;


    public BooleanCommand(final Integer opaque, final int code, final String message) {
        super(opaque);
        this.code = code;
        switch (this.code) {
        case HttpStatus.Success:
            this.setResponseStatus(ResponseStatus.NO_ERROR);
            break;
        default:
            this.setResponseStatus(ResponseStatus.ERROR);
            break;
        }
        this.message = message;
    }


    @Override
    public String getErrorMsg() {
        return this.message;
    }


    public int getCode() {
        return this.code;
    }


    @Override
    public void setErrorMsg(final String errorMsg) {
        this.message = errorMsg;

    }


    @Override
    public IoBuffer encode() {
        final byte[] bytes = ByteUtils.getBytes(this.message);
        final int messageLen = bytes == null ? 0 : bytes.length;
        final IoBuffer buffer =
                IoBuffer.allocate(11 + ByteUtils.stringSize(this.code) + ByteUtils.stringSize(this.getOpaque())
                        + ByteUtils.stringSize(messageLen) + messageLen);
        ByteUtils.setArguments(buffer, MetaEncodeCommand.RESULT_CMD, this.code, messageLen, this.getOpaque());
        if (bytes != null) {
            buffer.put(bytes);
        }
        buffer.flip();
        return buffer;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + this.code;
        result = prime * result + (this.message == null ? 0 : this.message.hashCode());
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
        final BooleanCommand other = (BooleanCommand) obj;
        if (this.code != other.code) {
            return false;
        }
        if (this.message == null) {
            if (other.message != null) {
                return false;
            }
        }
        else if (!this.message.equals(other.message)) {
            return false;
        }
        return true;
    }


    @Override
    public boolean isBoolean() {
        return true;
    }

}