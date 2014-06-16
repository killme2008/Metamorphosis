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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.ByteString;
import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.store.AppendCallback;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.TransactionRecoveryListener;
import com.taobao.metamorphosis.server.transaction.TransactionStore;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand;
import com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.MetaMBeanServer;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * 事务存储引擎
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-22
 * 
 */
public class JournalTransactionStore implements TransactionStore, JournalTransactionStoreMBean {

    private final JournalStore journalStore;

    private final Map<Object, Tx> inflightTransactions = new LinkedHashMap<Object, Tx>();
    private final Map<TransactionId, Tx> preparedTransactions = new LinkedHashMap<TransactionId, Tx>();
    private boolean doingRecover;

    /**
     * 事务操作接口
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-8-22
     * 
     */
    public static interface TxOperation {

        static final byte ADD_OP = 0;


        public byte getType();

    }

    public static class AddMsgOperation implements TxOperation {

        public MessageStore store;
        public long msgId;
        public PutCommand putCmd;


        @Override
        public byte getType() {
            return ADD_OP;
        }


        public AddMsgOperation(final MessageStore store, final long msgId, final PutCommand putCmd) {
            super();
            this.store = store;
            this.msgId = msgId;
            this.putCmd = putCmd;
        }

    }

    /**
     * 添加消息到store的位置和checksum
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-8-23
     * 
     */
    public static class AddMsgLocation extends Location {
        public final int checksum; // 校验和，整个消息的校验和，注意跟message的校验和区分

        public final String storeDesc; // 分区描述字符串


        public AddMsgLocation(final long offset, final int length, final int checksum, final String storeDesc) {
            super(offset, length);
            this.checksum = checksum;
            this.storeDesc = storeDesc;
        }

        private ByteBuffer buf;


        public static AddMsgLocation decode(final ByteBuffer buf) {
            if (!buf.hasRemaining()) {
                return null;
            }
            final long offset = buf.getLong();
            final int length = buf.getInt();
            final int checksum = buf.getInt();
            final int descLen = buf.getInt();
            final byte[] descBytes = new byte[descLen];
            buf.get(descBytes);
            final String desc = ByteUtils.getString(descBytes);
            return new AddMsgLocation(offset, length, checksum, desc);
        }


        /**
         * 消息位置序列化为:
         * <ul>
         * <li>8个字节的offset</li>
         * <li>4个字节的长度</li>
         * <li>4个字节checksum:这是指整个消息存储数据的checksum，跟message的checksum不同</li>
         * <li>4个字节长度，存储的分区名的长度</li>
         * <li>存储的分区名</li>
         * </ul>
         * 
         * @return
         */
        public ByteBuffer encode() {
            if (this.buf == null) {
                final byte[] storeDescBytes = ByteUtils.getBytes(this.storeDesc);
                final ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 8 + 4 + this.storeDesc.length());
                buf.putLong(this.getOffset());
                buf.putInt(this.getLength());
                buf.putInt(this.checksum);
                buf.putInt(storeDescBytes.length);
                buf.put(storeDescBytes);
                buf.flip();
                this.buf = buf;
            }
            return this.buf;
        }

    }

    /**
     * 事务内存对象，保存操作轨迹
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-8-22
     * 
     */
    public static class Tx {

        private final JournalLocation location;
        private final ConcurrentHashMap<MessageStore, Queue<TxOperation>> operations =
                new ConcurrentHashMap<MessageStore, Queue<TxOperation>>();


        JournalLocation getLocation() {
            return this.location;
        }


        public Tx(final JournalLocation location) {
            this.location = location;
        }


        public void add(final MessageStore store, final long msgId, final PutCommand putCmd) {
            final AddMsgOperation addMsgOperation = new AddMsgOperation(store, msgId, putCmd);
            Queue<TxOperation> ops = this.operations.get(store);
            if (ops == null) {
                ops = new ConcurrentLinkedQueue<TxOperation>();
                final Queue<TxOperation> oldOps = this.operations.putIfAbsent(store, ops);
                if (oldOps != null) {
                    ops = oldOps;
                }
            }
            ops.add(addMsgOperation);
        }


        public Map<MessageStore, List<Long>> getMsgIds() {
            final Map<MessageStore, List<Long>> rt = new LinkedHashMap<MessageStore, List<Long>>();
            for (final Map.Entry<MessageStore, Queue<TxOperation>> entry : this.operations.entrySet()) {
                final MessageStore store = entry.getKey();
                final Queue<TxOperation> opQueue = entry.getValue();
                final List<Long> ids = new ArrayList<Long>();
                rt.put(store, ids);
                for (final TxOperation to : opQueue) {
                    if (to.getType() == TxOperation.ADD_OP) {
                        ids.add(((AddMsgOperation) to).msgId);
                    }
                }
            }
            return rt;
        }


        public PutCommand[] getRequests() {
            final List<PutCommand> list = new ArrayList<PutCommand>(this.operations.size() * 2);
            for (final Map.Entry<MessageStore, Queue<TxOperation>> entry : this.operations.entrySet()) {
                for (final TxOperation op : entry.getValue()) {
                    if (op.getType() == TxOperation.ADD_OP) {
                        list.add(((AddMsgOperation) op).putCmd);
                    }
                }
            }
            final PutCommand[] rt = new PutCommand[list.size()];
            return list.toArray(rt);
        }


        public Map<MessageStore, List<PutCommand>> getPutCommands() {
            final Map<MessageStore, List<PutCommand>> rt = new LinkedHashMap<MessageStore, List<PutCommand>>();
            for (final Map.Entry<MessageStore, Queue<TxOperation>> entry : this.operations.entrySet()) {
                final MessageStore store = entry.getKey();
                final Queue<TxOperation> opQueue = entry.getValue();
                final List<PutCommand> ids = new ArrayList<PutCommand>();
                rt.put(store, ids);
                for (final TxOperation to : opQueue) {
                    if (to.getType() == TxOperation.ADD_OP) {
                        ids.add(((AddMsgOperation) to).putCmd);
                    }
                }
            }
            return rt;
        }


        public Map<MessageStore, Queue<TxOperation>> getOperations() {
            return this.operations;
        }

    }

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    static final Log log = LogFactory.getLog(JournalLocation.class);


    public JournalTransactionStore(final String dataPath, final MessageStoreManager storeManager,
            final MetaConfig metaConfig) throws Exception {
        this.journalStore =
                new JournalStore(dataPath, storeManager, this, metaConfig.getMaxCheckpoints(),
                    metaConfig.getFlushTxLogAtCommit());
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    JournalTransactionStore.this.makeCheckpoint();
                }
                catch (final Exception e) {
                    log.error("Execute checkpoint failed", e);
                }
            }
        }, metaConfig.getCheckpointInterval(), metaConfig.getCheckpointInterval(), TimeUnit.MILLISECONDS);
        MetaMBeanServer.registMBean(this, null);
    }


    @Override
    public void prepare(final TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (this.inflightTransactions) {
            tx = this.inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        final TransactionOperation to =
                TransactionOperation.newBuilder().setType(TransactionType.XA_PREPARE)
                .setTransactionId(txid.getTransactionKey()).setWasPrepared(false).build();
        // prepare,必须force
        final TxCommand msg =
                TxCommand.newBuilder().setCmdType(TxCommandType.TX_OP).setCmdContent(to.toByteString()).setForce(true)
                .build();
        this.journalStore.write(msg, null, tx.location, false);

        synchronized (this.preparedTransactions) {
            this.preparedTransactions.put(txid, tx);
        }
    }


    public JournalStore getJournalStore() {
        return this.journalStore;
    }


    // public void setJournalStore(final JournalStore journalStore) {
    // this.journalStore = journalStore;
    // }

    public void replayPrepare(final TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (this.inflightTransactions) {
            tx = this.inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        synchronized (this.preparedTransactions) {
            this.preparedTransactions.put(txid, tx);
        }
    }


    public Tx getInflyTx(final Object txid) {
        synchronized (this.inflightTransactions) {
            return this.inflightTransactions.get(txid);
        }
    }


    @Override
    public int getActiveTransactionCount() {
        int count = 0;
        synchronized (this.preparedTransactions) {
            count += this.preparedTransactions.size();
        }
        synchronized (this.inflightTransactions) {
            count += this.inflightTransactions.size();
        }
        return count;
    }


    Tx getPreparedTx(final Object txid) {
        synchronized (this.preparedTransactions) {
            return this.preparedTransactions.get(txid);
        }
    }


    public Tx getTx(final Object txid, final JournalLocation location) {
        synchronized (this.inflightTransactions) {
            Tx tx = this.inflightTransactions.get(txid);
            if (tx == null) {
                tx = new Tx(location);
                this.inflightTransactions.put(txid, tx);

            }
            return tx;
        }
    }


    @Override
    public void commit(final TransactionId txid, final boolean wasPrepared) throws IOException {
        final Tx tx;
        if (wasPrepared) {
            synchronized (this.preparedTransactions) {
                tx = this.preparedTransactions.remove(txid);
            }
        }
        else {
            synchronized (this.inflightTransactions) {
                tx = this.inflightTransactions.remove(txid);
            }
        }
        if (tx == null) {
            return;
        }
        // Append messages
        final Map<MessageStore, List<Long>> msgIds = tx.getMsgIds();
        final Map<MessageStore, List<PutCommand>> putCommands = tx.getPutCommands();
        final Map<String, AddMsgLocation> locations =
                new LinkedHashMap<String, JournalTransactionStore.AddMsgLocation>();

        final int count = msgIds.size();

        for (final Map.Entry<MessageStore, List<Long>> entry : msgIds.entrySet()) {
            final MessageStore msgStore = entry.getKey();
            final List<Long> ids = entry.getValue();
            final List<PutCommand> cmds = putCommands.get(msgStore);
            // Append message
            msgStore.append(ids, cmds, new AppendCallback() {

                @Override
                public void appendComplete(final Location location) {
                    // Calculate checksum
                    final int checkSum = CheckSum.crc32(MessageUtils.makeMessageBuffer(ids, cmds).array());
                    final String description = msgStore.getDescription();
                    // Store append location
                    synchronized (locations) {
                        locations.put(description, new AddMsgLocation(location.getOffset(), location.getLength(),
                            checkSum, description));
                        // 处理完成
                        if (locations.size() == count) {
                            // 将位置信息序列化，并作为tx
                            // command的附加数据存储，这部分数据的长度是固定的，因此可以在replay的时候更改
                            final ByteBuffer localtionBytes = AddMsgLocationUtils.encodeLocation(locations);

                            TxCommand msg = null;
                            // Log transaction
                            final int attachmentLen = localtionBytes.remaining();
                            if (txid.isXATransaction()) {
                                final TransactionOperation to = TransactionOperation.newBuilder() //
                                        .setType(TransactionType.XA_COMMIT) //
                                        .setTransactionId(txid.getTransactionKey()) //
                                        .setWasPrepared(wasPrepared) //
                                        .setDataLength(attachmentLen) // 设置附加数据长度
                                        .build();
                                msg =
                                        TxCommand.newBuilder().setCmdType(TxCommandType.TX_OP)
                                        .setCmdContent(to.toByteString()).build();
                            }
                            else {
                                final TransactionOperation to = TransactionOperation.newBuilder() //
                                        .setType(TransactionType.LOCAL_COMMIT) //
                                        .setTransactionId(txid.getTransactionKey()) //
                                        .setWasPrepared(wasPrepared) //
                                        .setDataLength(attachmentLen)// 设置附加数据长度
                                        .build();
                                msg =
                                        TxCommand.newBuilder().setCmdType(TxCommandType.TX_OP)
                                        .setCmdContent(to.toByteString()).build();
                            }
                            // 记录commit日志，并附加位置信息
                            try {
                                JournalTransactionStore.this.journalStore.write(msg, localtionBytes, tx.location, true);
                            }
                            catch (final IOException e) {
                                throw new RuntimeException("Write tx log failed", e);
                            }
                        }
                    }
                }
            });

        }

    }


    public Tx replayCommit(final TransactionId txid, final boolean wasPrepared) throws IOException {
        if (wasPrepared) {
            synchronized (this.preparedTransactions) {
                return this.preparedTransactions.remove(txid);
            }
        }
        else {
            synchronized (this.inflightTransactions) {
                return this.inflightTransactions.remove(txid);
            }
        }
    }


    @Override
    public void rollback(final TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (this.inflightTransactions) {
            tx = this.inflightTransactions.remove(txid);
        }
        if (tx == null) {
            synchronized (this.preparedTransactions) {
                tx = this.preparedTransactions.remove(txid);
            }
        }
        if (tx != null) {
            if (txid.isXATransaction()) {
                final TransactionOperation to = TransactionOperation.newBuilder() //
                        .setType(TransactionType.XA_ROLLBACK) //
                        .setTransactionId(txid.getTransactionKey()) //
                        .setWasPrepared(false) //
                        .build();
                final TxCommand msg =
                        TxCommand.newBuilder().setCmdType(TxCommandType.TX_OP).setCmdContent(to.toByteString()).build();
                this.journalStore.write(msg, null, tx.location, true);
            }
            else {
                final TransactionOperation to = TransactionOperation.newBuilder() //
                        .setType(TransactionType.LOCAL_ROLLBACK) //
                        .setTransactionId(txid.getTransactionKey()) //
                        .setWasPrepared(false) //
                        .build();
                final TxCommand msg =
                        TxCommand.newBuilder().setCmdType(TxCommandType.TX_OP).setCmdContent(to.toByteString()).build();
                this.journalStore.write(msg, null, tx.location, true);
            }
        }
    }


    public void replayRollback(final TransactionId txid) throws IOException {
        boolean inflight = false;
        synchronized (this.inflightTransactions) {
            inflight = this.inflightTransactions.remove(txid) != null;
        }
        if (!inflight) {
            synchronized (this.preparedTransactions) {
                this.preparedTransactions.remove(txid);
            }
        }
    }


    @Override
    public void init() {
    }


    @Override
    public void dispose() {
        this.scheduledExecutorService.shutdown();
        try {
            this.journalStore.close();
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public synchronized void recover(final TransactionRecoveryListener listener) throws IOException {
        // 所有本地事务都回滚
        Map<Object, Tx> copyMap = null;
        synchronized (this.inflightTransactions) {
            copyMap = new HashMap<Object, JournalTransactionStore.Tx>(this.inflightTransactions);
            // this.inflightTransactions.clear();
        }
        for (final Map.Entry<Object, Tx> entry : copyMap.entrySet()) {
            this.rollback((TransactionId) entry.getKey());
            if (log.isDebugEnabled()) {
                log.debug("Rollback inflight transaction:" + entry.getKey());
            }
        }
        // 恢复XA中的prepared事务
        this.doingRecover = true;
        try {
            Map<TransactionId, Tx> txs = null;
            synchronized (this.preparedTransactions) {
                txs = new LinkedHashMap<TransactionId, Tx>(this.preparedTransactions);
            }

            for (final Map.Entry<TransactionId, Tx> entry : txs.entrySet()) {
                final Object txid = entry.getKey();
                final Tx tx = entry.getValue();
                listener.recover((XATransactionId) txid, tx.getRequests());
            }
        }
        finally {
            this.doingRecover = false;
        }
    }


    /**
     * 添加消息，为了保证添加顺序，这里不得不加锁
     */
    @Override
    public void addMessage(final MessageStore store, final long msgId, final PutCommand putCmd, JournalLocation location)
            throws IOException {
        if (location == null) {
            // 非重放，添加put日志
            final AppendMessageCommand appendCmd =
                    AppendMessageCommand.newBuilder().setMessageId(msgId)
                    .setPutCommand(ByteString.copyFrom(putCmd.encode().array())).build();
            final TxCommand txCommand =
                    TxCommand.newBuilder().setCmdType(TxCommandType.APPEND_MSG).setCmdContent(appendCmd.toByteString())
                    .build();
            final Tx tx = this.getInflyTx(putCmd.getTransactionId());
            if (tx != null) {
                location = this.journalStore.write(txCommand, null, tx.location, false);
            }
            else {
                location = this.journalStore.write(txCommand, null, null, false);
            }
        }
        final Tx tx = this.getTx(putCmd.getTransactionId(), location);
        tx.add(store, msgId, putCmd);
    }


    @Override
    public void makeCheckpoint() throws Exception {
        this.journalStore.checkpoint();

    }


    public JournalLocation checkpoint() throws IOException {
        // 查找存活事务中最早开始的那个的位置，作为checkpoint保存起来，下次恢复只要从checkpoint位置开始恢复即可
        JournalLocation rc = null;
        synchronized (this.inflightTransactions) {
            for (final Iterator<Tx> iter = this.inflightTransactions.values().iterator(); iter.hasNext();) {
                final Tx tx = iter.next();
                final JournalLocation location = tx.location;
                if (rc == null || rc.compareTo(location) > 0) {
                    rc = location;
                }
            }
        }
        synchronized (this.preparedTransactions) {
            for (final Iterator<Tx> iter = this.preparedTransactions.values().iterator(); iter.hasNext();) {
                final Tx tx = iter.next();
                final JournalLocation location = tx.location;
                if (rc == null || rc.compareTo(location) > 0) {
                    rc = location;
                }
            }
            return rc;
        }
    }


    public boolean isDoingRecover() {
        return this.doingRecover;
    }

}