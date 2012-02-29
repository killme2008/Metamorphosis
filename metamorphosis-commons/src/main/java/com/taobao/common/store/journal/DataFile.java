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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;


/**
 * 代表了一个数据文件
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
class DataFile {
    private final File file;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    protected FileChannel fc;
    private final int number;
    private volatile long currentPos;


    /**
     * 构造函数，会打开指定的文件，并且将指针指向文件结尾
     * 
     * @param file
     * @throws IOException
     */
    DataFile(final File file, final int number) throws IOException {
        this(file, number, false);
    }


    /**
     * 构造函数，会打开指定的文件，并且将指针指向文件结尾
     * 
     * @param file
     * @throws IOException
     */
    DataFile(final File file, final int number, final boolean force) throws IOException {
        this.file = file;
        this.fc = new RandomAccessFile(file, force ? "rws" : "rw").getChannel();
        // 指针移到最后
        this.fc.position(this.fc.size());
        this.currentPos = this.fc.position();
        this.number = number;
    }


    int getNumber() {
        return this.number;
    }


    /**
     * 获得文件的大小
     * 
     * @return 文件的大小
     * @throws IOException
     */
    long getLength() throws IOException {
        return this.currentPos;
    }


    long position() throws IOException {
        return this.currentPos;
    }


    void forward(final long offset) {
        this.currentPos += offset;
    }


    void sync(final Condition condition) throws Exception {
        while (this.fc.position() < this.currentPos) {
            condition.await(1, TimeUnit.SECONDS);
        }
        this.fc.force(true);
    }


    /**
     * 获取文件最后修改时间
     * 
     * @return
     * @throws IOException
     */
    long lastModified() throws IOException {
        return this.file.lastModified();
    }


    /**
     * 删除文件
     * 
     * @return 是否删除成功
     * @throws IOException
     */
    boolean delete() throws IOException {
        this.close();
        return this.file.delete();
    }


    /**
     * 强制将数据写回硬盘
     * 
     * @throws IOException
     */
    void force() throws IOException {
        this.fc.force(true);
    }


    /**
     * 关闭文件
     * 
     * @throws IOException
     */
    void close() throws IOException {
        this.fc.close();
    }


    /**
     * 从文件读取数据到bf，直到读满或者读到文件结尾。 <br />
     * 文件的指针会向后移动bf的大小
     * 
     * @param bf
     * @throws IOException
     */
    void read(final ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            final int l = this.fc.read(bf);
            if (l < 0) {
                break;
            }
        }
    }


    /**
     * 从文件的制定位置读取数据到bf，直到读满或者读到文件结尾。 <br />
     * 文件指针不会移动
     * 
     * @param bf
     * @param offset
     * @throws IOException
     */
    void read(final ByteBuffer bf, final long offset) throws IOException {
        int size = 0;
        int l = 0;
        while (bf.hasRemaining()) {
            l = this.fc.read(bf, offset + size);
            if (l < 0) {
                // 数据还未写入，忙等待
                if (offset < this.currentPos) {
                    continue;
                }
                else {
                    break;
                }
            }
            size += l;
        }
    }


    /**
     * 写入bf长度的数据到文件，文件指针会向后移动
     * 
     * @param bf
     * @return 写入后的文件position
     * @throws IOException
     */
    long write(final ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            final int l = this.fc.write(bf);
            if (l < 0) {
                break;
            }
        }
        return this.fc.position();
    }


    /**
     * 从指定位置写入bf长度的数据到文件，文件指针<b>不会</b>向后移动
     * 
     * @param offset
     * @param bf
     * @throws IOException
     */
    void write(final long offset, final ByteBuffer bf) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = this.fc.write(bf, offset + size);
            size += l;
            if (l < 0) {
                break;
            }
        }
    }


    /**
     * 对文件增加一个引用计数
     * 
     * @return 增加后的引用计数
     */
    int increment() {
        return this.referenceCount.incrementAndGet();
    }


    int increment(final int n) {
        return this.referenceCount.addAndGet(n);
    }


    /**
     * 对文件减少一个引用计数
     * 
     * @return 减少后的引用计数
     */
    int decrement() {
        return this.referenceCount.decrementAndGet();
    }


    int decrement(final int n) {
        return this.referenceCount.addAndGet(-n);
    }


    /**
     * 文件是否还在使用（引用计数是否是0了）
     * 
     * @return 文件是否还在使用
     */
    boolean isUnUsed() {
        return this.getReferenceCount() <= 0;
    }


    /**
     * 获得引用计数的值
     * 
     * @return 引用计数的值
     */
    int getReferenceCount() {
        return this.referenceCount.get();
    }


    @Override
    public String toString() {
        String result = null;
        try {
            result =
                    this.file.getName() + " , length = " + this.getLength() + " refCount = " + this.referenceCount
                            + " position:" + this.fc.position();
        }
        catch (final IOException e) {
            result = e.getMessage();
        }
        return result;
    }
}