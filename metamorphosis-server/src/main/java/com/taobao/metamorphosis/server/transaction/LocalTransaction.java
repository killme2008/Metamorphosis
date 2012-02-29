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
package com.taobao.metamorphosis.server.transaction;

import java.io.IOException;

import javax.transaction.xa.XAException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 本地事务实现
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-23
 * 
 */
public class LocalTransaction extends Transaction {

    /**
     * 
     */
    private static final long serialVersionUID = 3488724356970710207L;

    private static final Log LOG = LogFactory.getLog(LocalTransaction.class);

    private final transient TransactionStore transactionStore;
    private final LocalTransactionId xid;
    private final transient SessionContext context;


    public LocalTransaction(final TransactionStore transactionStore, final LocalTransactionId xid,
            final SessionContext context) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.context = context;
    }


    @Override
    public void commit(final boolean onePhase) throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("commit: " + this.xid);
        }

        // Get ready for commit.
        try {
            this.prePrepare();
        }
        catch (final XAException e) {
            throw e;
        }
        catch (final Throwable e) {
            LOG.warn("COMMIT FAILED: ", e);
            this.rollback();
            // Let them know we rolled back.
            final XAException xae = new XAException("COMMIT FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(e);
            throw xae;
        }

        this.setState(Transaction.FINISHED_STATE);
        this.context.getTransactions().remove(this.xid);
        try {
            this.transactionStore.commit(this.getTransactionId(), false);
        }
        catch (final Throwable t) {
            LOG.warn("Store COMMIT FAILED: ", t);
            this.rollback();
            final XAException xae = new XAException("STORE COMMIT FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(t);
            throw xae;
        }
    }


    @Override
    public void rollback() throws XAException, IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("rollback: " + this.xid);
        }
        this.setState(Transaction.FINISHED_STATE);
        this.context.getTransactions().remove(this.xid);
        this.transactionStore.rollback(this.getTransactionId());
    }


    @Override
    public int prepare() throws XAException {
        final XAException xae = new XAException("Prepare not implemented on Local Transactions.");
        xae.errorCode = XAException.XAER_RMERR;
        throw xae;
    }


    @Override
    public TransactionId getTransactionId() {
        return this.xid;
    }


    @Override
    public Log getLog() {
        return LOG;
    }
}