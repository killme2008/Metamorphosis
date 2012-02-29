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
 * 退出命令，客户端发送此命令后，服务器将主动关闭连接
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class QuitCommand extends AbstractRequestCommand {

    static final long serialVersionUID = -1L;


    public QuitCommand() {
        super(null, Integer.MAX_VALUE);
    }

    static final IoBuffer QUIT_BUF = IoBuffer.wrap((MetaEncodeCommand.QUIT_CMD + "\r\n").getBytes());


    @Override
    public IoBuffer encode() {
        return QUIT_BUF.slice();
    }

}