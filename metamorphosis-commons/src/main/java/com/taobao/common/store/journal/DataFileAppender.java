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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.common.store.journal.JournalStore.InflyWriteData;
import com.taobao.common.store.util.BytesKey;


public class DataFileAppender {

    private volatile boolean shutdown = false;
    private boolean running = false;
    private Thread thread;
    private final Lock enqueueLock = new ReentrantLock();
    private final Condition notEmpty = this.enqueueLock.newCondition();
    private final Condition empty = this.enqueueLock.newCondition();
    protected int maxWriteBatchSize;
    protected final Map<BytesKey, InflyWriteData> inflyWrites = new ConcurrentHashMap<BytesKey, InflyWriteData>(256);
    private WriteBatch nextWriteBatch;
    private final JournalStore journal;


    public DataFileAppender(final JournalStore journalStore) {
        this.maxWriteBatchSize = journalStore.maxWriteBatchSize;
        this.journal = journalStore;
    }


    public OpItem remove(final OpItem opItem, final BytesKey bytesKey, final boolean sync) throws IOException {
        if (this.shutdown) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        // sync = true;
        final WriteCommand writeCommand = new WriteCommand(bytesKey, opItem, null, sync);
        return this.enqueueTryWait(opItem, sync, writeCommand);
    }


    public OpItem store(final OpItem opItem, final BytesKey bytesKey, final byte[] data, final boolean sync)
            throws IOException {
        if (this.shutdown) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        opItem.key = bytesKey.getData();
        opItem.length = data.length;
        final WriteCommand writeCommand = new WriteCommand(bytesKey, opItem, data, sync);
        return this.enqueueTryWait(opItem, sync, writeCommand);
    }


    private OpItem enqueueTryWait(final OpItem opItem, final boolean sync, final WriteCommand writeCommand)
            throws IOException {
        final WriteBatch batch = this.enqueue(writeCommand, sync);
        if (sync) {
            try {
                batch.latch.await();
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            final IOException exception = batch.exception;
            if (exception != null) {
                throw exception;

            }
        }
        return opItem;
    }


    public void close() {
        this.enqueueLock.lock();
        try {
            if (!this.shutdown) {
                this.shutdown = true;
                this.running = false;
                this.empty.signalAll();
            }
        }
        finally {
            this.enqueueLock.unlock();
        }
        while (this.thread.isAlive()) {
            try {
                this.thread.join();
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    public void processQueue() {
        while (true) {
            WriteBatch batch = null;
            this.enqueueLock.lock();
            try {
                while (true) {
                    if (this.nextWriteBatch != null) {
                        batch = this.nextWriteBatch;
                        this.nextWriteBatch = null;
                        break;
                    }
                    if (this.shutdown) {
                        return;
                    }
                    try {
                        this.empty.await();
                    }
                    catch (final InterruptedException e) {
                        break;
                    }

                }
                this.notEmpty.signalAll();
            }
            finally {
                this.enqueueLock.unlock();
            }
            if (batch != null) {
                final DataFile dataFile = batch.dataFile;
                final LogFile logFile = batch.logFile;
                final List<WriteCommand> cmdList = batch.cmdList;
                try {
                    this.writeDataAndLog(batch, dataFile, logFile, cmdList);
                    this.processRemove(batch, dataFile, logFile);
                }
                finally {
                    batch.latch.countDown();
                }
            }

        }
    }


    private void processRemove(final WriteBatch batch, final DataFile df, final LogFile lf) {

        if (df != null && lf != null) {
            df.decrement(batch.removeOPCount);
            this.enqueueLock.lock();
            try {
                if (df.getLength() >= JournalStore.FILE_SIZE && df.isUnUsed()) {
                    if (this.journal.dataFile == df) { // 判断如果是当前文件，生成新的
                        this.journal.newDataFile();
                    }
                    this.journal.dataFiles.remove(Integer.valueOf(df.getNumber()));
                    this.journal.logFiles.remove(Integer.valueOf(df.getNumber()));
                    // System.out.println("delete " + df.getNumber());
                    // System.out.println(batch.cmdList.get(0).opItem);
                    df.delete();
                    lf.delete();
                }
            }
            catch (final Exception e) {
                if (e instanceof IOException) {
                    batch.exception = (IOException) e;
                }
                else {
                    batch.exception = new IOException(e);
                }
            }
            finally {
                this.enqueueLock.unlock();
            }
        }
    }


    public byte[] getDataFromInFlyWrites(final BytesKey key) {
        final InflyWriteData inflyWriteData = this.inflyWrites.get(key);
        if (inflyWriteData != null && inflyWriteData.count > 0) {
            return inflyWriteData.data;
        }
        else {
            return null;
        }

    }


    private void writeDataAndLog(final WriteBatch batch, final DataFile dataFile, final LogFile logFile,
            final List<WriteCommand> dataList) {
        ByteBuffer dataBuf = null;
        // Contains op add
        if (batch.dataSize > 0) {
            dataBuf = ByteBuffer.allocate(batch.dataSize);
        }
        final ByteBuffer opBuf = ByteBuffer.allocate(dataList.size() * OpItem.LENGTH);
        for (final WriteCommand cmd : dataList) {
            opBuf.put(cmd.opItem.toByte());
            if (cmd.opItem.op == OpItem.OP_ADD) {
                dataBuf.put(cmd.data);
            }
        }
        if (dataBuf != null) {
            dataBuf.flip();
        }
        opBuf.flip();
        try {
            if (dataBuf != null) {
                dataFile.write(dataBuf);
            }
            logFile.write(opBuf);
        }
        catch (final IOException e) {
            batch.exception = e;
        }
        this.enqueueLock.lock();
        try {
            // 非同步，从inflyWrites缓存中移除
            for (final WriteCommand cmd : dataList) {
                if (!cmd.sync && cmd.opItem.op == OpItem.OP_ADD) {
                    final InflyWriteData inflyWriteData = this.inflyWrites.get(cmd.bytesKey);
                    if (inflyWriteData != null) {
                        // decrease reference count
                        inflyWriteData.count--;
                        // remove it if there is no reference
                        if (inflyWriteData.count <= 0) {
                            this.inflyWrites.remove(cmd.bytesKey);
                        }
                    }
                }
            }
        }
        finally {
            this.enqueueLock.unlock();
        }

    }

    final Condition notSync = this.enqueueLock.newCondition();


    public void sync() {
        this.enqueueLock.lock();
        try {
            while (this.nextWriteBatch != null) {
                try {
                    this.notEmpty.await();
                }
                catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            for (final DataFile df : this.journal.dataFiles.values()) {
                try {
                    df.sync(this.notSync);
                }
                catch (final Exception e) {

                }
            }
        }
        finally {
            this.enqueueLock.unlock();
        }
    }


    private WriteBatch enqueue(final WriteCommand writeCommand, final boolean sync) throws IOException {
        WriteBatch result = null;
        this.enqueueLock.lock();
        try {
            // 如果没有启动，则先启动appender线程
            this.startAppendThreadIfNessary();
            if (this.nextWriteBatch == null) {
                result = this.newWriteBatch(writeCommand);
                this.empty.signalAll();
            }
            else {
                if (this.nextWriteBatch.canAppend(writeCommand)) {
                    this.nextWriteBatch.append(writeCommand);
                    result = this.nextWriteBatch;
                }
                else {
                    while (this.nextWriteBatch != null) {
                        try {
                            this.notEmpty.await();
                        }
                        catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    result = this.newWriteBatch(writeCommand);
                    this.empty.signalAll();
                }
            }
            if (!sync) {
                final InflyWriteData inflyWriteData = this.inflyWrites.get(writeCommand.bytesKey);
                switch (writeCommand.opItem.op) {
                case OpItem.OP_ADD:
                    if (inflyWriteData == null) {
                        this.inflyWrites.put(writeCommand.bytesKey, new InflyWriteData(writeCommand.data));
                    }
                    else {
                        // update and increase reference count;
                        inflyWriteData.data = writeCommand.data;
                        inflyWriteData.count++;
                    }
                    break;
                case OpItem.OP_DEL:
                    // 无条件删除
                    if (inflyWriteData != null) {
                        this.inflyWrites.remove(writeCommand.bytesKey);
                    }
                }

            }
            return result;
        }
        finally {
            this.enqueueLock.unlock();
        }

    }


    private WriteBatch newWriteBatch(final WriteCommand writeCommand) throws IOException {
        WriteBatch result = null;
        // 设置offset和number
        if (writeCommand.opItem.op == OpItem.OP_ADD) {
            // 预先判断一次
            if (this.journal.indices.containsKey(writeCommand.bytesKey)) {
                throw new IOException("发现重复的key");
            }
            final DataFile dataFile = this.getDataFile();
            writeCommand.opItem.offset = dataFile.position();
            writeCommand.opItem.number = dataFile.getNumber();
            // 移动dataFile指针
            dataFile.forward(writeCommand.data.length);
            this.nextWriteBatch = new WriteBatch(writeCommand, dataFile, this.journal.logFile);
            result = this.nextWriteBatch;
        }
        else {
            final DataFile dataFile = this.journal.dataFiles.get(writeCommand.opItem.number);
            final LogFile logFile = this.journal.logFiles.get(writeCommand.opItem.number);
            if (dataFile != null && logFile != null) {
                this.nextWriteBatch = new WriteBatch(writeCommand, dataFile, logFile);
                result = this.nextWriteBatch;
            }
            else {
                // System.out.println(this.journal.dataFiles);
                throw new IOException("日志文件已经被删除，编号为" + writeCommand.opItem.number);
            }
        }
        return result;
    }


    private void startAppendThreadIfNessary() {
        if (!this.running) {
            this.running = true;
            this.thread = new Thread() {
                @Override
                public void run() {
                    DataFileAppender.this.processQueue();
                }
            };
            this.thread.setPriority(Thread.MAX_PRIORITY);
            this.thread.setDaemon(true);
            this.thread.setName("Store4j file appender");
            this.thread.start();
        }
    }


    private DataFile getDataFile() throws IOException {
        DataFile dataFile = this.journal.dataFile;
        if (dataFile.getLength() >= JournalStore.FILE_SIZE) { // 满了
            dataFile = this.journal.newDataFile();
        }
        return dataFile;
    }

    private class WriteCommand {
        final BytesKey bytesKey;
        final OpItem opItem;
        final byte[] data;
        final boolean sync;


        public WriteCommand(final BytesKey bytesKey, final OpItem opItem, final byte[] data, final boolean sync) {
            super();
            this.bytesKey = bytesKey;
            this.opItem = opItem;
            this.data = data;
            this.sync = sync;
        }


        @Override
        public String toString() {
            return this.opItem.toString();
        }
    }

    /**
     * 一次批量写入记录
     * 
     * @author dennis
     * 
     */
    private class WriteBatch {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<WriteCommand> cmdList = new ArrayList<WriteCommand>();
        // 删除操作的个数
        int removeOPCount;
        final DataFile dataFile;
        final LogFile logFile;
        // 写入的data大小
        int dataSize;
        // DataFile写入的起点
        long offset = -1;
        volatile IOException exception;
        // 写入文件的编号
        final int number;


        public WriteBatch(final WriteCommand writeCommand, final DataFile dataFile, final LogFile logFile) {
            super();
            this.dataFile = dataFile;
            this.number = writeCommand.opItem.number;
            this.logFile = logFile;
            switch (writeCommand.opItem.op) {
            case OpItem.OP_DEL:
                this.removeOPCount++;
                break;
            case OpItem.OP_ADD:
                this.offset = writeCommand.opItem.offset;
                this.dataSize += writeCommand.data.length;
                this.dataFile.increment();
                break;
            default:
                throw new RuntimeException("Unknow op type " + writeCommand.opItem);
            }
            this.cmdList.add(writeCommand);

        }


        public boolean canAppend(final WriteCommand command) throws IOException {
            switch (command.opItem.op) {
            case OpItem.OP_DEL:
                // 删除不在本次batch的文件上，不可合并
                if (command.opItem.number != this.number) {
                    return false;
                }
                break;
            case OpItem.OP_ADD:
                // 文件不能太大
                if (this.dataFile.getLength() + command.data.length >= JournalStore.FILE_SIZE) {
                    return false;
                }
                // 一次batch不能太大
                if (this.dataSize + command.data.length >= DataFileAppender.this.maxWriteBatchSize) {
                    return false;
                }
                break;
            default:
                throw new RuntimeException("Unknow op type " + command.opItem);
            }

            return true;
        }


        public void append(final WriteCommand writeCommand) throws IOException {
            switch (writeCommand.opItem.op) {
            case OpItem.OP_ADD:
                // 添加操作需要设定offset
                // 1、第一个op是remove的情况
                if (this.offset == -1) {
                    this.offset = this.dataFile.position();
                    writeCommand.opItem.offset = this.dataFile.position();
                    writeCommand.opItem.number = this.dataFile.getNumber();
                    this.dataFile.forward(writeCommand.data.length);
                    this.dataSize += writeCommand.data.length;
                }
                else {
                    writeCommand.opItem.offset = this.offset + this.dataSize;
                    writeCommand.opItem.number = this.dataFile.getNumber();
                    this.dataFile.forward(writeCommand.data.length);
                    this.dataSize += writeCommand.data.length;
                }
                this.dataFile.increment();
                break;
            case OpItem.OP_DEL:
                this.removeOPCount++;
                break;
            default:
                throw new RuntimeException("Unknow op type " + writeCommand.opItem);
            }
            this.cmdList.add(writeCommand);
        }

    }
}