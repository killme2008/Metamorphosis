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
 * 协议编码接口和常量
 * 
 * @author boyan
 * @Date 2011-6-2
 * 
 */
public interface MetaEncodeCommand {
    /**
     * 编码协议
     * 
     * @return 编码后的buffer
     */
    public IoBuffer encode();

    byte SPACE = (byte) ' ';
    byte[] CRLF = { '\r', '\n' };
    public String GET_CMD = "get";
    public String RESULT_CMD = "result";
    public String OFFSET_CMD = "offset";
    public String VALUE_CMD = "value";
    public String PUT_CMD = "put";
    public String SYNC_CMD = "sync";
    public String QUIT_CMD = "quit";
    public String VERSION_CMD = "version";
    public String STATS_CMD = "stats";
    public String TRANS_CMD = "transaction";
}