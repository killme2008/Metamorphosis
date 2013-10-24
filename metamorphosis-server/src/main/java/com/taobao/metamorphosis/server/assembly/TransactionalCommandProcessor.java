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
package com.taobao.metamorphosis.server.assembly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.service.timer.HashedWheelTimer;
import com.taobao.gecko.service.timer.Timeout;
import com.taobao.gecko.service.timer.Timer;
import com.taobao.gecko.service.timer.TimerTask;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.CommandProcessorFilter;
import com.taobao.metamorphosis.server.exception.MetamorphosisException;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.network.SessionContextImpl;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.HeuristicTransactionJournal;
import com.taobao.metamorphosis.server.transaction.LocalTransaction;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.server.transaction.TransactionRecoveryListener;
import com.taobao.metamorphosis.server.transaction.TransactionStore;
import com.taobao.metamorphosis.server.transaction.XATransaction;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.MetaMBeanServer;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metamorphosis.utils.NamedThreadFactory;


/**
 * 事务命令处理器
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public class TransactionalCommandProcessor extends CommandProcessorFilter implements TransactionalCommandProcessorMBean {

    private static final Log LOG = LogFactory.getLog(TransactionalCommandProcessor.class);

    private final TransactionStore transactionStore;
    private final HeuristicTransactionJournal heuristicTransactionJournal;
    // The prepared XA transactions.
    private final Map<TransactionId, XATransaction> xaTransactions = new LinkedHashMap<TransactionId, XATransaction>();

    /**
     * 手工提交或者回滚的事务
     */
    private Map<TransactionId, XATransaction> xaHeuristicTransactions =
            new LinkedHashMap<TransactionId, XATransaction>();

    private final MessageStoreManager storeManager;
    private final IdWorker idWorker;
    private final StatsManager statsManager;

    private final Timer txTimeoutTimer;

    private final MetaConfig metaConfig;

    private final ScheduledExecutorService scheduledExecutorService;


    public TransactionalCommandProcessor(final MetaConfig metaConfig, final MessageStoreManager storeManager,
            final IdWorker idWorker, final CommandProcessor next, final TransactionStore transactionStore,
            final StatsManager stasManager) {
        super(next);
        try {
            this.heuristicTransactionJournal = new HeuristicTransactionJournal(metaConfig.getDataLogPath());
        }
        catch (final IOException e) {
            throw new MetamorphosisServerStartupException("Initialize HeuristicTransactionJournal failed", e);
        }
        this.metaConfig = metaConfig;
        this.idWorker = idWorker;
        this.storeManager = storeManager;
        this.transactionStore = transactionStore;
        this.statsManager = stasManager;
        this.txTimeoutTimer =
                new HashedWheelTimer(new NamedThreadFactory("Tx-Timeout-Timer"), 500, TimeUnit.MILLISECONDS, 512,
                    metaConfig.getMaxTxTimeoutTimerCapacity());
        MetaMBeanServer.registMBean(this, null);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.scheduleWriteHeuristicTransactions(metaConfig);
    }


    HeuristicTransactionJournal getHeuristicTransactionJournal() {
        return this.heuristicTransactionJournal;
    }


    private void scheduleWriteHeuristicTransactions(final MetaConfig metaConfig) {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    TransactionalCommandProcessor.this.heuristicTransactionJournal
                    .write(TransactionalCommandProcessor.this.xaHeuristicTransactions);
                }
                catch (final Exception e) {
                    log.error("Write xaHeuristicTransactions to journal failed", e);
                }

            }
        }, metaConfig.getCheckpointInterval(), metaConfig.getCheckpointInterval(), TimeUnit.MILLISECONDS);
    }


    @Override
    public TransactionId[] getPreparedTransactions(final SessionContext context, final String uniqueQualifier)
            throws Exception {
        final List<TransactionId> txs = new ArrayList<TransactionId>();
        synchronized (this.xaTransactions) {
            for (final Iterator<XATransaction> iter = this.xaTransactions.values().iterator(); iter.hasNext();) {
                final XATransaction tx = iter.next();
                // Only tx that the unique qualifier is equals to the request
                // one.
                if (tx.isPrepared() && this.isValidTx(uniqueQualifier, tx)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("prepared transaction: " + tx.getTransactionId());
                    }
                    txs.add(tx.getTransactionId());
                }
            }
        }
        synchronized (this.xaHeuristicTransactions) {
            // 手工处理的事务，都是prepare状态的xa事务
            for (final Iterator<XATransaction> iter = this.xaHeuristicTransactions.values().iterator(); iter.hasNext();) {
                final XATransaction tx = iter.next();
                // Only tx that the unique qualifier is equals to the request
                // one.
                if (this.isValidTx(uniqueQualifier, tx)) {
                    txs.add(tx.getTransactionId());
                }
            }
        }
        final XATransactionId rc[] = new XATransactionId[txs.size()];
        txs.toArray(rc);
        if (LOG.isDebugEnabled()) {
            LOG.debug("prepared transacton list size: " + rc.length);
        }
        return rc;
    }


    private boolean isValidTx(final String uniqueQualifier, final XATransaction tx) {
        assert tx.getUniqueQualifier() != null;
        // uniqueQualifier should not be null,but it may be sent by old clients.
        return tx.getUniqueQualifier().equals(uniqueQualifier) || uniqueQualifier == null;
    }


    @Override
    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds)
            throws Exception {
        Transaction transaction = null;
        if (xid.isXATransaction()) {
            this.statsManager.statsTxBegin(true, 1);
            transaction = null;
            synchronized (this.xaTransactions) {
                transaction = this.xaTransactions.get(xid);
                if (transaction != null) {
                    return;
                }
                transaction = new XATransaction(this, this.transactionStore, (XATransactionId) xid);
                this.xaTransactions.put(xid, (XATransaction) transaction);
            }
        }
        else {
            this.statsManager.statsTxBegin(false, 1);
            final Map<TransactionId, Transaction> transactionMap = context.getTransactions();
            transaction = transactionMap.get(xid);
            if (transaction != null) {
                return;
            }
            transaction = new LocalTransaction(this.transactionStore, (LocalTransactionId) xid, context);
            transactionMap.put(xid, transaction);
        }
        if (transaction != null) {
            // 设置事务超时
            this.setTxTimeout(context, transaction, seconds);
        }
    }


    @Override
    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        final Transaction transaction = this.getTransaction(context, xid);
        return transaction.prepare();
    }


    @Override
    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase)
            throws Exception {
        this.statsManager.statsTxCommit(1);
        final Transaction transaction = this.getTransaction(context, xid);
        transaction.commit(onePhase);
    }


    @Override
    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        this.statsManager.statsTxRollback(1);
        final Transaction transaction = this.getTransaction(context, xid);
        transaction.rollback();
    }


    @Override
    public void forgetTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        if (xid == null || !xid.isXATransaction()) {
            final String errMsg = xid != null ? xid.toString() + " is not a valid xid" : "Null xid";
            final XAException e = new XAException(errMsg);
            e.errorCode = XAException.XAER_NOTA;
            throw e;
        }
        synchronized (this.xaHeuristicTransactions) {
            this.xaHeuristicTransactions.remove(xid);
        }
    }


    @Override
    public void init() {
        super.init();
        // this.remotingServer.registerProcessor(TransactionCommand.class, new
        // TransactionProcessor(this));
        this.recoverPreparedTransactions();

    }


    @Override
    public void dispose() {
        super.dispose();
        this.transactionStore.dispose();
        if (this.txTimeoutTimer != null) {
            this.txTimeoutTimer.stop();
        }
        this.scheduledExecutorService.shutdownNow();
        try {
            this.heuristicTransactionJournal.write(this.xaHeuristicTransactions);
            this.heuristicTransactionJournal.close();
        }
        catch (final Exception e) {
            log.error("Close heuristicTransactionJournal failed", e);
        }
    }


    void recoverPreparedTransactions() {
        try {
            final SessionContextImpl context = new SessionContextImpl(null, null);
            context.setInRecoverMode(true);
            this.recoverHeuristicTransactions();
            this.transactionStore.recover(new TransactionRecoveryListener() {

                @Override
                public void recover(final XATransactionId xid, final PutCommand[] putCmds) {
                    try {
                        TransactionalCommandProcessor.this.beginTransaction(context, xid, 0);
                        for (final PutCommand cmd : putCmds) {
                            TransactionalCommandProcessor.this.processPutCommand(cmd, context, null);
                        }
                        TransactionalCommandProcessor.this.prepareTransaction(context, xid);
                    }
                    catch (final Throwable e) {
                        throw new RuntimeException(e);
                    }

                }
            });
        }
        catch (final Throwable e) {
            throw new MetamorphosisServerStartupException("Recover prepared transactions failed", e);
        }
    }


    @SuppressWarnings("unchecked")
    void recoverHeuristicTransactions() throws Exception {
        this.xaHeuristicTransactions = (Map<TransactionId, XATransaction>) this.heuristicTransactionJournal.read();
        if (this.xaHeuristicTransactions == null) {
            this.xaHeuristicTransactions = new LinkedHashMap<TransactionId, XATransaction>();
        }
        for (final XATransaction tx : this.xaHeuristicTransactions.values()) {
            // 设置transient变量
            tx.setBrokerProcessor(this);
            tx.setTransactionStore(this.transactionStore);
        }
    }


    @Override
    public void processPutCommand(final PutCommand cmd, final SessionContext context, final PutCallback cb)
            throws Exception {
        Transaction transaction = null;
        if (cmd.getTransactionId() != null) {
            transaction = this.getTransaction(context, cmd.getTransactionId());
        }
        if (transaction != null) {
            transaction.setTransactionInUse();
            if (context.isInRecoverMode()) {
                // 恢复模式，不需要处理
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(HttpStatus.Forbidden, "The broker is in recover mode.", cmd
                        .getOpaque()));
                }
                return;
            }
            final String topic = cmd.getTopic();
            final int partition = cmd.getPartition();

            final String partitionString = this.metaConfig.getBrokerId() + "-" + partition;
            if (partition == Partition.RandomPartiton.getPartition()) {
                this.statsManager.statsPutFailed(topic, partitionString, 1);
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError,
                        "Invalid partition for transaction command:" + partition, cmd.getOpaque()));
                }
                return;
            }
            final MessageStore store = this.storeManager.getOrCreateMessageStore(topic, partition);
            if (store == null) {
                this.statsManager.statsPutFailed(topic, partitionString, 1);
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError,
                        "Could not get or create message store for topic=" + topic + ",partition=" + partition, cmd
                        .getOpaque()));
                }
                return;
            }
            final long msgId = this.idWorker.nextId();
            this.transactionStore.addMessage(store, msgId, cmd, null);
            this.statsManager.statsPut(topic, partitionString, 1);
            if (cb != null) {
                cb.putComplete(new BooleanCommand(HttpStatus.Success, this.genPutResultString(partition, msgId, -1),
                    cmd.getOpaque()));
            }
        }
        else {
            super.processPutCommand(cmd, context, cb);
        }
    }


    /**
     * 返回形如"messageId partition offset"的字符号，返回给客户端
     * 
     * @param partition
     * @param messageId
     * @param offset
     * @return
     */
    private String genPutResultString(final int partition, final long messageId, final long offset) {
        final StringBuilder sb =
                new StringBuilder(ByteUtils.stringSize(offset) + ByteUtils.stringSize(messageId)
                    + ByteUtils.stringSize(partition) + 2);
        sb.append(messageId).append(" ").append(partition).append(" ").append(offset);
        return sb.toString();
    }


    @Override
    public Transaction getTransaction(final SessionContext context, final TransactionId xid)
            throws MetamorphosisException, XAException {
        Transaction transaction = null;
        if (xid.isXATransaction()) {
            synchronized (this.xaTransactions) {
                transaction = this.xaTransactions.get(xid);
            }
        }
        else {
            transaction = context.getTransactions().get(xid);
        }

        if (transaction != null) {
            return transaction;
        }

        // 判断是否人工提交或者回滚过了
        if (xid.isXATransaction()) {
            synchronized (this.xaHeuristicTransactions) {
                transaction = this.xaHeuristicTransactions.get(xid);
            }
            if (transaction != null) {
                switch (transaction.getState()) {
                case Transaction.HEURISTIC_COMMIT_STATE:
                    XAException e = new XAException("XA transaction '" + xid + "' has been heuristically committed.");
                    e.errorCode = XAException.XA_HEURCOM;
                    throw e;
                case Transaction.HEURISTIC_ROLLBACK_STATE:
                    e = new XAException("XA transaction '" + xid + "' has been heuristically rolled back.");
                    e.errorCode = XAException.XA_HEURRB;
                    throw e;
                case Transaction.HEURISTIC_COMPLETE_STATE:
                    e = new XAException("XA transaction '" + xid + "' has been heuristically completed.");
                    e.errorCode = XAException.XA_HEURHAZ;
                    throw e;
                default:
                    log.warn("Invalid transaction state in xaHeuristicTransactions:" + transaction.getState());
                    // 应该不会出现这种情况
                    break;
                }
            }
        }
        if (xid.isXATransaction()) {
            final XAException e = new XAException("XA transaction '" + xid + "' has not been started.");
            e.errorCode = XAException.XAER_NOTA;
            throw e;
        }
        else {
            throw new MetamorphosisException("Local transaction '" + xid + "' has not been started.");
        }
    }


    @Override
    public void removeTransaction(final XATransactionId xid) {
        synchronized (this.xaTransactions) {
            this.xaTransactions.remove(xid);
        }
    }

    static final Log log = LogFactory.getLog(TransactionalCommandProcessor.class);


    /**
     * 设置XA事务超时
     * 
     * @param ctx
     * @param xid
     * @throws MetamorphosisException
     * @throws XAException
     */
    private void setTxTimeout(final SessionContext ctx, final Transaction tx, int seconds)
            throws MetamorphosisException, XAException {
        if (tx == null) {
            return;
        }
        if (tx.getTimeoutRef() != null) {
            return;
        }
        // 0则表示永不超时
        // TODO 是否采用默认的最大超时时间？
        if (seconds <= 0) {
            return;
        }
        if (seconds > this.metaConfig.getMaxTxTimeoutInSeconds()) {
            seconds = this.metaConfig.getMaxTxTimeoutInSeconds();
        }
        tx.setTimeoutRef(this.txTimeoutTimer.newTimeout(new TimerTask() {

            @Override
            public void run(final Timeout timeout) throws Exception {
                // 没有prepared的到期事务要回滚
                // 需要与XATransaction.prepare做同步隔离
                synchronized (tx) {
                    if (!tx.isPrepared() && tx.getState() != Transaction.FINISHED_STATE) {
                        log.warn("XA transaction " + tx.getTransactionId() + " is timeout,it is rolled back.");
                        tx.rollback();
                    }
                }
            }
        }, seconds, TimeUnit.SECONDS));
    }


    /**
     * 以下为暴露给JMX MBean的接口方法
     */

    @Override
    public String[] getPreparedTransactions() throws Exception {
        final TransactionId[] ids = this.getPreparedTransactions(null, null);
        final String[] rt = new String[ids.length];
        for (int i = 0; i < ids.length; i++) {
            rt[i] = ids[i].getTransactionKey();
        }
        return rt;
    }


    @Override
    public int getPreparedTransactionCount() throws Exception {
        return this.getPreparedTransactions(null, null).length;
    }


    @Override
    public void commitTransactionHeuristically(final String txKey, final boolean onePhase) throws Exception {
        final TransactionId xid = TransactionId.valueOf(txKey);
        if (xid.isNull() || !xid.isXATransaction()) {
            return;
        }
        final Transaction transaction = this.getTransaction(null, xid);
        if (transaction == null || !transaction.isPrepared()) {
            return;
        }
        this.commitTransaction(null, xid, onePhase);
        transaction.setState(Transaction.HEURISTIC_COMMIT_STATE);
        synchronized (this.xaHeuristicTransactions) {
            this.xaHeuristicTransactions.put(xid, (XATransaction) transaction);
        }
    }


    public Map<TransactionId, XATransaction> getXAHeuristicTransactions() {
        return this.xaHeuristicTransactions;
    }


    @Override
    public void completeTransactionHeuristically(final String txKey) throws Exception {
        final TransactionId xid = TransactionId.valueOf(txKey);
        if (xid.isNull() || !xid.isXATransaction()) {
            return;
        }
        final Transaction transaction = this.getTransaction(null, xid);
        if (transaction == null || !transaction.isPrepared()) {
            return;
        }
        synchronized (this.xaTransactions) {
            this.xaTransactions.remove(xid);
        }
        transaction.setState(Transaction.HEURISTIC_COMPLETE_STATE);
        synchronized (this.xaHeuristicTransactions) {
            this.xaHeuristicTransactions.put(xid, (XATransaction) transaction);
        }
    }


    @Override
    public void rollbackTransactionHeuristically(final String txKey) throws Exception {
        final TransactionId xid = TransactionId.valueOf(txKey);
        if (xid.isNull() || !xid.isXATransaction()) {
            return;
        }
        final Transaction transaction = this.getTransaction(null, xid);
        if (transaction == null || !transaction.isPrepared()) {
            return;
        }
        this.rollbackTransaction(null, xid);
        if (transaction != null) {
            transaction.setState(Transaction.HEURISTIC_ROLLBACK_STATE);
            synchronized (this.xaHeuristicTransactions) {
                this.xaHeuristicTransactions.put(xid, (XATransaction) transaction);
            }
        }
    }

}