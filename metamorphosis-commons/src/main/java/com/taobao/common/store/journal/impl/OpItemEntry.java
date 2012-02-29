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
 *   boyan <killme2008@gmail.com>
 */
package com.taobao.common.store.journal.impl;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import com.taobao.common.store.journal.OpItem;


/**
 * 
 * 存储在硬盘上的OpItem
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-20 上午11:26:37
 */

public class OpItemEntry {
    public static final int SIZE = 33 + 1;
    private OpItem opItem;
    private boolean deleted;
    private byte channelIndex;
    // 公用的读取deleted字段的 buffer
    static final byte[] deltedBuffer = new byte[1];


    public boolean isLoaded() {
        return this.opItem != null;
    }


    public void unload() {
        this.opItem = null; // 消除引用，让GC回收
    }


    public byte getChannelIndex() {
        return channelIndex;
    }


    public void setChannelIndex(final byte channelIndex) {
        this.channelIndex = channelIndex;
    }


    public void load(final MappedByteBuffer mappedByteBuffer, final int offset, final boolean loadItem)
            throws IOException {
        // 已经删除，不用继续读
        if (this.deleted) {
            return;
        }
        mappedByteBuffer.position(offset);
        if (!loadItem) {
            final byte data = mappedByteBuffer.get();
            this.deleted = (data == (byte) 1 ? true : false);
        }
        else {
            final byte[] bytes = new byte[SIZE];
            mappedByteBuffer.get(bytes, 0, SIZE);
            this.deleted = (bytes[0] == (byte) 1 ? true : false);
            this.opItem = new OpItem();
            this.opItem.parse(bytes, 1, bytes.length - 1);
        }
    }


    public byte[] encode() {
        if (this.opItem != null) {
            final byte[] buffer = new byte[OpItemEntry.SIZE];
            if (this.deleted) {
                buffer[0] = 1;
            }
            else {
                buffer[0] = 0;
            }
            final byte[] data = this.opItem.toByte();
            System.arraycopy(data, 0, buffer, 1, data.length);
            return buffer;
        }
        else {
            return null;
        }
    }


    public OpItemEntry(final OpItem opItem, final boolean deleted) {
        super();
        this.opItem = opItem;
        this.deleted = deleted;
    }


    public OpItem getOpItem() {
        return opItem;
    }


    public boolean isDeleted() {
        return deleted;
    }


    public void setDeleted(final boolean deleted) {
        this.deleted = deleted;
    }
}