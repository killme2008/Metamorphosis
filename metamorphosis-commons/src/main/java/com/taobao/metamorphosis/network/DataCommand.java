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
 * 应答命令，协议格式如下： value total-length opaque\r\n data,其中data的结构如下：
 * <ul>
 * <li>4个字节的消息数据长度（可能包括属性）</li>
 * <li>4个字节的check sum</li>
 * <li>8个字节的消息id</li>
 * <li>4个字节的flag</li>
 * <li>消息数据，如果有属性，则为：
 * <ul>
 * <li>4个字节的属性长度+ 消息属性 + payload</li>
 * </ul>
 * </li> 否则为：
 * <ul>
 * <li>payload</li>
 * <ul>
 * </li>
 * </ul>
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public class DataCommand extends AbstractResponseCommand {
    private final byte[] data;
    static final long serialVersionUID = -1L;


    public byte[] getData() {
        return this.data;
    }


    public DataCommand(final byte[] data, final Integer opaque) {
        super(opaque);
        this.data = data;
    }


    @Override
    public boolean isBoolean() {
        return false;
    }


    @Override
    public IoBuffer encode() {
        // 不做任何事情，发送data command由transferTo替代
        return null;
    }

}