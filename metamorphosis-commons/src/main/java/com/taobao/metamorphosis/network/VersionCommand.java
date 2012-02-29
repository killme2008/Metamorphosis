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
import com.taobao.gecko.core.command.kernel.HeartBeatRequestCommand;


/**
 * 查询服务器版本，也用于心跳检测，协议：version opaque\r\n
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class VersionCommand extends AbstractRequestCommand implements HeartBeatRequestCommand {
    static final long serialVersionUID = -1L;


    public VersionCommand(final Integer opaque) {
        super(null, opaque);
    }


    @Override
    public IoBuffer encode() {
        return IoBuffer.wrap((MetaEncodeCommand.VERSION_CMD + " " + this.getOpaque() + "\r\n").getBytes());
    }

}