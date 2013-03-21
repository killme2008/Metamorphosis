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

/**
 * 数据存入的位置
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-22
 * 
 */
public class Location {
    protected final long offset;
    protected final int length;

    public static Location InvalidLocaltion = new Location(-1, -1);


    protected Location(final long offset, final int length) {
        super();
        this.offset = offset;
        this.length = length;
    }


    public static Location create(long offset, int length) {
        if (offset < 0 || length < 0) {
            return InvalidLocaltion;
        }
        return new Location(offset, length);
    }


    public boolean isValid() {
        return this != InvalidLocaltion;
    }


    public long getOffset() {
        return this.offset;
    }


    public int getLength() {
        return this.length;
    }

}