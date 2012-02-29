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
package com.taobao.metamorphosis.server.store;

import java.io.File;


/**
 * 超过一定时间的删除策略
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public class DiscardDeletePolicy implements DeletePolicy {
    public static final String NAME = "delete";
    // 最长保存时间，单位毫秒
    protected long maxReservedTime;


    void setMaxReservedTime(final long maxReservedTime) {
        this.maxReservedTime = maxReservedTime;
    }


    long getMaxReservedTime() {
        return this.maxReservedTime;
    }


    /**
     * 删除文件
     */
    @Override
    public void process(final File file) {
        file.delete();
    }


    @Override
    public boolean canDelete(final File file, final long checkTimestamp) {
        return checkTimestamp - file.lastModified() > this.maxReservedTime;
    }


    @Override
    public void init(final String... values) {
        if (values[0].endsWith("m")) {
            // minutes
            final int minutes = this.getValue(values[0]);
            this.maxReservedTime = minutes * 60L * 1000L;
        }
        else if (values[0].endsWith("s")) {
            // seconds
            final int seconds = this.getValue(values[0]);
            this.maxReservedTime = seconds * 1000L;
        }
        else if (values[0].endsWith("h")) {
            // hours
            final int hours = this.getValue(values[0]);
            this.maxReservedTime = hours * 3600L * 1000L;
        }
        else {
            // default is hours
            final int hours = Integer.parseInt(values[0]);
            this.maxReservedTime = hours * 3600L * 1000L;
        }
    }


    private int getValue(final String v) {
        return Integer.valueOf(v.substring(0, v.length() - 1));
    }


    @Override
    public String name() {
        return NAME;
    }

}