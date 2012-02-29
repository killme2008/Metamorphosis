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
 *   dogun (yuexuqiang at gmail.com)
 */
package com.taobao.common.store.journal;

import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * 一个日志记录 操作+数据key+数据文件编号+偏移量+长度
 * 
 * @author dogun (yuexuqiang at gmail.com)
 * 
 */
public class OpItem {
    public static final byte OP_ADD = 1;
    public static final byte OP_DEL = 2;

    public static final int KEY_LENGTH = 16;
    public static final int LENGTH = KEY_LENGTH + 1 + 4 + 8 + 4;

    byte op;
    byte[] key;
    int number;
    volatile long offset;
    int length;


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.key);
        result = prime * result + this.length;
        result = prime * result + this.number;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + this.op;
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
        final OpItem other = (OpItem) obj;
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        if (this.length != other.length) {
            return false;
        }
        if (this.number != other.number) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.op != other.op) {
            return false;
        }
        return true;
    }


    /**
     * 将一个操作转换成字节数组
     * 
     * @return 字节数组
     */
    public byte[] toByte() {
        final byte[] data = new byte[LENGTH];
        final ByteBuffer bf = ByteBuffer.wrap(data);
        bf.put(this.key);
        bf.put(this.op);
        bf.putInt(this.number);
        bf.putLong(this.offset);
        bf.putInt(this.length);
        bf.flip();
        return bf.array();
    }


    public byte getOp() {
        return this.op;
    }


    public void setOp(final byte op) {
        this.op = op;
    }


    public byte[] getKey() {
        return this.key;
    }


    public void setKey(final byte[] key) {
        this.key = key;
    }


    public int getNumber() {
        return this.number;
    }


    public void setNumber(final int number) {
        this.number = number;
    }


    public long getOffset() {
        return this.offset;
    }


    public void setOffset(final long offset) {
        this.offset = offset;
    }


    public int getLength() {
        return this.length;
    }


    public void setLength(final int length) {
        this.length = length;
    }


    /**
     * 通过字节数组构造成一个操作日志
     * 
     * @param data
     */
    public void parse(final byte[] data) {
        this.parse(data, 0, data.length);
    }


    public void parse(final byte[] data, final int offset, final int length) {
        final ByteBuffer bf = ByteBuffer.wrap(data, offset, length);
        this.key = new byte[16];
        bf.get(this.key);
        this.op = bf.get();
        this.number = bf.getInt();
        this.offset = bf.getLong();
        this.length = bf.getInt();
    }


    public void parse(final ByteBuffer bf) {
        this.key = new byte[16];
        bf.get(this.key);
        this.op = bf.get();
        this.number = bf.getInt();
        this.offset = bf.getLong();
        this.length = bf.getInt();
    }


    @Override
    public String toString() {
        return "OpItem number:" + this.number + ", op:" + (int) this.op + ", offset:" + this.offset + ", length:"
                + this.length;
    }
}