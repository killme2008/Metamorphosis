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


/**
 * 统计信息查询 格式：</br> stats item opaque\r\n
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class StatsCommand extends AbstractRequestCommand {
    static final long serialVersionUID = -1L;
    // 统计项目名称，可以为空
    private final String item;


    public StatsCommand(final Integer opaque, final String item) {
        super(null, opaque);
        this.item = item;
    }


    public String getItem() {
        return this.item;
    }


    @Override
    public IoBuffer encode() {
        if (StringUtils.isBlank(this.item)) {
            return IoBuffer.wrap((MetaEncodeCommand.STATS_CMD + " " + " " + this.getOpaque() + "\r\n").getBytes());
        }
        else {
            final IoBuffer buf = IoBuffer.allocate(9 + this.item.length() + ByteUtils.stringSize(this.getOpaque()));
            ByteUtils.setArguments(buf, MetaEncodeCommand.STATS_CMD, this.item, this.getOpaque());
            buf.flip();
            return buf;
        }
    }

}