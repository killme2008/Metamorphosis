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
package com.taobao.metamorphosis.server.transaction.store;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.CodecFactory.Decoder;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.store.AppendCallback;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore.AddMsgLocation;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore.Tx;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand;
import com.taobao.metamorphosis.server.utils.FileUtils;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * 事务存储引擎
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-17
 * 
 */
public class JournalStore implements Closeable {

    private final ConcurrentHashMap<Integer/* number */, DataFile> dataFiles =
            new ConcurrentHashMap<Integer, DataFile>();

    private final AtomicInteger number = new AtomicInteger(0);

    private DataFile currDataFile;

    private final File transactionsDir;

    private final Lock writeLock = new ReentrantLock();

    private final String FILE_PREFIX = "redo.";

    private final JournalTransactionStore transactionStore;

    private final MessageStoreManager storeManager;

    private final Checkpoint checkpoint;

    // 每个日志文件最大大小为64M,,67108864
    static int MAX_FILE_SIZE = Integer.parseInt(System.getProperty("meta.tx_journal_file_size", "67108864"));

    // 日志刷盘设置，0表示让操作系统决定，1表示每次commit都刷盘，2表示每隔一秒刷盘一次
    private final int flushTxLogAtCommit;

    // 修改过的datafile列表
    private final Set<DataFile> modifiedDataFiles = new HashSet<DataFile>();


    DataFile getCurrDataFile() {
        return this.currDataFile;
    }


    ConcurrentHashMap<Integer, DataFile> getDataFiles() {
        return this.dataFiles;
    }

    private ScheduledExecutorService scheduledExecutorService;


    public JournalStore(final String path, final MessageStoreManager storeManager,
            final JournalTransactionStore transactionStore, final int maxCheckpoints, final int flushTxLogAtCommit)
            throws Exception {
        FileUtils.makesureDir(new File(path));
        this.transactionsDir = new File(path + File.separator + "transactions");
        FileUtils.makesureDir(this.transactionsDir);
        this.storeManager = storeManager;
        this.transactionStore = transactionStore;
        this.checkpoint = new Checkpoint(path, maxCheckpoints);
        if (flushTxLogAtCommit < 0 || flushTxLogAtCommit > 2) {
            throw new IllegalArgumentException("Invalid flushLogTxCommit,only supports 0,1 or 2");
        }
        this.flushTxLogAtCommit = flushTxLogAtCommit;
        this.recover();
        if (this.flushTxLogAtCommit == 2) {
            // 2表示每秒刷盘一次
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        JournalStore.this.force();
                    }
                    catch (final IOException e) {
                        log.error("force datafiles failed", e);
                    }

                }
            }, 1, 1, TimeUnit.SECONDS);
        }

    }


    private boolean isNeedForce(final TxCommand txCmd, final boolean commitOrRollback) {
        // 等于1的时候才需要每次prepare/commit都force
        return (commitOrRollback || txCmd.getForce()) && this.flushTxLogAtCommit == 1;
    }


    private void force() throws IOException {
        this.writeLock.lock();
        try {
            for (final DataFile df : this.modifiedDataFiles) {
                df.force();
            }
        }
        finally {
            try {
                this.modifiedDataFiles.clear();
            }
            finally {
                this.writeLock.unlock();
            }
        }
    }


    /**
     * 写入命令到事务日志
     * 
     * @param msg
     *            事务命令
     * @param attachment
     *            事务命令的附加数据
     * @param location
     *            写入的位置
     * @param committedOrRollback
     *            是否是提交或者回滚命令
     * @return
     * @throws IOException
     */
    public JournalLocation write(final TxCommand msg, final ByteBuffer attachment, final JournalLocation location,
            final boolean committedOrRollback) throws IOException {
        final byte[] data = msg.toByteArray();
        final ByteBuffer buf = ByteBuffer.allocate(4 + data.length + (attachment != null ? attachment.remaining() : 0));
        buf.putInt(data.length);
        buf.put(data);
        if (attachment != null) {
            buf.put(attachment);
        }
        buf.flip();
        DataFile dataFile = null;
        this.writeLock.lock();
        try {
            dataFile = this.getDataFile(location);
            final long offset = dataFile.position();
            dataFile.write(buf);
            // 提交或者回滚，递减计数
            if (committedOrRollback) {
                dataFile.decrement();
                // 加入变更队列，等待刷盘
                if (this.flushTxLogAtCommit == 2) {
                    this.modifiedDataFiles.add(dataFile);
                }
            }
            this.maybeRoll(dataFile);
            return new JournalLocation(dataFile.getNumber(), offset);
        }
        finally {
            this.writeLock.unlock();
            final boolean force = this.isNeedForce(msg, committedOrRollback);
            if (force) {
                this.force(dataFile);
            }
        }

    }


    private void force(final DataFile df) throws IOException {
        if (df == null) {
            return;
        }
        this.writeLock.lock();
        try {
            df.force();
        }
        finally {
            this.writeLock.unlock();
        }
    }


    @Override
    public void close() throws IOException {
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdown();
        }
        this.checkpoint.close();
        this.writeLock.lock();
        try {
            for (final DataFile df : this.dataFiles.values()) {
                try {
                    df.close();
                }
                catch (final Exception e) {
                    log.warn("close error:" + df, e);
                }
            }
            this.dataFiles.clear();
            this.currDataFile = null;
        }
        finally {
            this.writeLock.unlock();
        }

    }


    private void maybeRoll(final DataFile dataFile) throws IOException {
        // 文件超过大小并且不再被引用，则删除之
        if (dataFile.getLength() > MAX_FILE_SIZE && dataFile.isUnUsed()) {
            // 如果要删除的是当前文件，需要生成一个新文件
            if (dataFile == this.currDataFile) {
                this.newDataFile();
            }
            this.dataFiles.remove(dataFile.getNumber());
            dataFile.delete();
        }
    }


    private DataFile getDataFile(final JournalLocation location) throws IOException {
        DataFile dataFile = null;
        if (location == null) {
            dataFile = this.currDataFile;
            // 如果文件超过大小，则生成一个新文件
            if (dataFile.getLength() > MAX_FILE_SIZE) {
                dataFile = this.newDataFile();
            }
            // 递增计数
            dataFile.increment();
        }
        else {
            dataFile = this.dataFiles.get(location.number);
        }
        return dataFile;
    }

    private static final Decoder decoer = new MetamorphosisWireFormatType().newCodecFactory().getDecoder();

    static final Log log = LogFactory.getLog(JournalStore.class);


    private void recover() throws IOException {
        log.info("Begin to recover transaction journal...");
        final File[] ls = this.transactionsDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.startsWith(JournalStore.this.FILE_PREFIX);
            }
        });

        // 按照序号升序排序
        Arrays.sort(ls, new Comparator<File>() {

            @Override
            public int compare(final File o1, final File o2) {
                return JournalStore.this.getFileNumber(o1) - JournalStore.this.getFileNumber(o2);
            }

        });

        final JournalLocation cp = this.checkpoint.getRecentCheckpoint();
        // 4个字节的长度buffer
        final ByteBuffer lenBuf = ByteBuffer.allocate(4);
        DataFile dataFile = null;
        for (int i = 0; i < ls.length; i++) {
            final File file = ls[i];
            if (file.isFile() && file.canRead()) {
                // 从checkpoint开始回放
                if (cp == null || this.getFileNumber(file) >= cp.number) {
                    dataFile = this.recoverFile(cp, ls, lenBuf, i, file);
                }
            }
            else {
                log.info(file.getName() + " is not a valid transaction journal store file");
            }
        }

        if (dataFile == null) {
            this.currDataFile = this.newDataFile();
        }
        else {
            this.currDataFile = dataFile;
            this.number.set(dataFile.getNumber());
        }
        log.info("Recover transaction journal successfully");
    }


    /**
     * 返回最近的checkpoint
     * 
     * @return
     */
    public JournalLocation getRecentCheckpoint() {
        return this.checkpoint.getRecentCheckpoint();
    }


    /**
     * 设置一个checkpoint，下次回放将从最近设置的checkpoint开始
     */
    public void checkpoint() throws Exception {
        this.checkpoint.check(this.transactionStore.checkpoint());
    }


    private DataFile recoverFile(final JournalLocation cp, final File[] ls, final ByteBuffer lenBuf, final int i,
            final File file) throws IOException {
        final int number = this.getFileNumber(file);
        // 读数据的起点
        long readOffset = 0;
        final DataFile dataFile = new DataFile(file, number);

        // 如果当前恢复的文件是checkpoint对应的文件，则从checkpoint指定的offset开始读取
        if (cp != null && dataFile.getNumber() == cp.number) {
            readOffset = cp.offset;
        }

        final long startMs = System.currentTimeMillis();
        while (true) {
            lenBuf.clear();
            final long cmdOffset = readOffset;
            dataFile.read(lenBuf, readOffset);
            if (!lenBuf.hasRemaining()) {
                lenBuf.flip();
                final int cmdBufLen = lenBuf.getInt();
                final ByteBuffer cmdBuf = ByteBuffer.allocate(cmdBufLen);
                dataFile.read(cmdBuf, 4 + readOffset);
                if (!cmdBuf.hasRemaining()) {
                    cmdBuf.flip();
                    int attachmentLen = 0;
                    try {
                        attachmentLen = this.processCmd(number, cmdOffset, cmdBuf, dataFile);
                    }
                    catch (final Exception e) {
                        log.error("Process tx command failed", e);
                        // 回放失败，跳出循环，后续的事务日志将被truncate
                        break;
                        // throw new IllegalStateException(e);
                    }
                    readOffset += 4;
                    readOffset += cmdBufLen + attachmentLen;
                }
                else {
                    // 没读满cmdBuf，跳出循环
                    break;
                }
            }
            else {
                // 没读满lenBuf，跳出循环
                break;
            }
        }
        // 最后一个命令不完整，truncate掉
        long truncated = 0;
        if (readOffset != dataFile.position()) {
            truncated = dataFile.position() - readOffset;
            dataFile.truncate(readOffset);
        }
        log.info("Recovery transaction journal " + file.getAbsolutePath() + " succeeded in "
                + (System.currentTimeMillis() - startMs) / 1000 + " seconds. " + truncated + " bytes truncated.");

        if (dataFile.getLength() > MAX_FILE_SIZE && dataFile.isUnUsed()) {
            dataFile.delete();
            return null;
        }
        else {
            this.dataFiles.put(number, dataFile);
        }
        return dataFile;
    }


    /**
     * 重放事务日志，返回附加数据长度
     * 
     * @param number
     * @param offset
     * @param cmdBuf
     * @param dataFile
     * @return
     * @throws Exception
     */
    private int processCmd(final int number, final long offset, final ByteBuffer cmdBuf, final DataFile dataFile)
            throws Exception {
        final byte[] data = new byte[cmdBuf.remaining()];
        cmdBuf.get(data);
        final TxCommand cmd = TxCommand.parseFrom(data);
        if (cmd != null) {
            switch (cmd.getCmdType()) {
            case APPEND_MSG:
                return this.appendMsg(number, offset, cmd, dataFile);
            case TX_OP:
                return this.replayTx(offset, cmdBuf.capacity(), cmd, dataFile);
            }
        }
        return 0;
    }


    private int replayTx(final long offset, final long cmdBufLen, final TxCommand txCmd, final DataFile dataFile)
            throws InvalidProtocolBufferException {
        final TransactionOperation command = TransactionOperation.parseFrom(txCmd.getCmdContent());
        final TransactionId xid = TransactionId.valueOf(command.getTransactionId());
        try {
            // 重放事务日志
            switch (command.getType()) {
            case XA_PREPARE:
                this.transactionStore.replayPrepare(xid);
                break;
            case XA_COMMIT:
            case LOCAL_COMMIT:
                final Tx tx = this.transactionStore.replayCommit(xid, command.getWasPrepared());
                if (tx == null) {
                    break;
                }
                if (tx.getOperations().isEmpty()) {
                    break;
                }

                final Map<MessageStore, List<Long>> ids = tx.getMsgIds();
                final Map<MessageStore, List<PutCommand>> putCmds = tx.getPutCommands();
                // 获取附加数据，添加消息的位置信息
                final int attachmentLen = command.getDataLength();
                final ByteBuffer buf = ByteBuffer.allocate(attachmentLen);
                // 附加数据的索引位置，起点＋4个字节的cmd长度＋cmd本身长度
                final long dataOffset = 4 + offset + cmdBufLen;
                dataFile.read(buf, dataOffset);
                buf.flip();
                final Map<String, AddMsgLocation> locations = AddMsgLocationUtils.decodeLocations(buf);

                final AtomicBoolean replayed = new AtomicBoolean(false);
                final AtomicInteger counter = new AtomicInteger();
                if (ids != null && !ids.isEmpty()) {
                    for (final Map.Entry<MessageStore, List<Long>> entry : ids.entrySet()) {
                        final MessageStore msgStore = entry.getKey();
                        final AddMsgLocation addedLocation = locations.get(msgStore.getDescription());
                        // 没有添加消息，需要重新添加
                        final List<Long> idList = entry.getValue();
                        final List<PutCommand> cmdList = putCmds.get(msgStore);
                        if (addedLocation == null) {
                            counter.incrementAndGet();
                            msgStore.append(idList, cmdList, new AppendCallback() {
                                @Override
                                public void appendComplete(final Location newLocation) {
                                    replayed.set(true);
                                    final int checksum =
                                            CheckSum.crc32(MessageUtils.makeMessageBuffer(idList, cmdList).array());
                                    locations.put(msgStore.getDescription(),
                                        new AddMsgLocation(newLocation.getOffset(), newLocation.getLength(), checksum,
                                            msgStore.getDescription()));
                                    counter.decrementAndGet();
                                }
                            });

                        }
                        else {
                            // 尝试重放
                            counter.incrementAndGet();
                            msgStore.replayAppend(addedLocation.getOffset(), addedLocation.getLength(),
                                addedLocation.checksum, idList, cmdList, new AppendCallback() {
                                    @Override
                                    public void appendComplete(final Location newLocation) {
                                        // 如果重放的时候更新了位置，则需要更新位置信息
                                        if (newLocation != null) {
                                            replayed.set(true);
                                            locations.put(msgStore.getDescription(),
                                                new AddMsgLocation(newLocation.getOffset(), newLocation.getLength(),
                                                    addedLocation.checksum, addedLocation.storeDesc));
                                        }
                                        counter.decrementAndGet();
                                    }
                                });

                        }
                    }
                }

                // 如果有重放，覆写位置信息
                if (replayed.get()) {
                    // 等待回调完成
                    while (counter.get() > 0) {
                        Thread.sleep(50);
                    }
                    dataFile.write(dataOffset, AddMsgLocationUtils.encodeLocation(locations));
                    dataFile.force();
                }
                // 返回附加数据大小
                dataFile.decrement();
                return attachmentLen;
            case LOCAL_ROLLBACK:
            case XA_ROLLBACK:
                this.transactionStore.replayRollback(xid);
                dataFile.decrement();
                break;
            default:
                throw new IOException("Invalid journal command type: " + command.getType());
            }
        }
        catch (final IOException e) {
            log.error("Recovery Failure: Could not replay: " + txCmd + ", reason: " + e, e);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return 0;

    }


    private int appendMsg(final int number, final long offset, final TxCommand cmd, final DataFile dataFile)
            throws InvalidProtocolBufferException, IOException {
        final AppendMessageCommand appendCmd = AppendMessageCommand.parseFrom(cmd.getCmdContent());
        final PutCommand putCmd =
                (PutCommand) decoer.decode(IoBuffer.wrap(appendCmd.getPutCommand().toByteArray()), null);
        final MessageStore store = this.storeManager.getOrCreateMessageStore(putCmd.getTopic(), putCmd.getPartition());

        if (this.transactionStore.getInflyTx(putCmd.getTransactionId()) == null) {
            dataFile.increment();
        }
        this.transactionStore.addMessage(store, appendCmd.getMessageId(), putCmd, new JournalLocation(number, offset));
        return 0;
    }


    /**
     * 生成一个新的数据文件
     * 
     * @throws FileNotFoundException
     */
    protected DataFile newDataFile() throws IOException {
        final int n = this.number.incrementAndGet();
        this.currDataFile =
                new DataFile(new File(this.transactionsDir + File.separator + this.FILE_PREFIX + n), n, false);
        this.dataFiles.put(Integer.valueOf(n), this.currDataFile);
        log.info("生成新文件：" + this.currDataFile);
        return this.currDataFile;
    }


    private int getFileNumber(final File file) {
        final int number = Integer.parseInt(file.getName().substring(this.FILE_PREFIX.length()));
        return number;
    }
}