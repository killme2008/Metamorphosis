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
import java.io.Serializable;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;


/**
 * XAÊÂÎñ
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-24
 * 
 */
public class XATransaction extends Transaction implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7059382594250215928L;

    private static final Log LOG = LogFactory.getLog(XATransaction.class);

    private transient TransactionStore transactionStore;
    private XATransactionId xid;
    private transient CommandProcessor brokerProcessor;


    public String getUniqueQualifier() {
        return this.xid.getUniqueQualifier();
    }

    public XATransaction() {
        super();
    }


    public XATransaction(final CommandProcessor brokerProcessor, final TransactionStore transactionStore,
            final XATransactionId xid) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.brokerProcessor = brokerProcessor;
        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction new/begin : " + xid);
        }
    }


    public TransactionStore getTransactionStore() {
        return this.transactionStore;
    }


    public void setTransactionStore(final TransactionStore transactionStore) {
        this.transactionStore = transactionStore;
    }


    public XATransactionId getXid() {
        return this.xid;
    }


    public void setXid(final XATransactionId xid) {
        this.xid = xid;
    }


    public CommandProcessor getBrokerProcessor() {
        return this.brokerProcessor;
    }


    public void setBrokerProcessor(final CommandProcessor brokerProcessor) {
        this.brokerProcessor = brokerProcessor;
    }


    @Override
    public void commit(final boolean onePhase) throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction commit: " + this.xid);
        }

        switch (this.getState()) {
        case START_STATE:
            // 1 phase commit, no work done.
            this.checkForPreparedState(onePhase);
            this.setStateFinished();
            break;
        case IN_USE_STATE:
            // 1 phase commit, work done.
            this.checkForPreparedState(onePhase);
            this.doPrePrepare();
            this.setStateFinished();
            this.storeCommit(this.getTransactionId(), false);
            break;
        case PREPARED_STATE:
            // 2 phase commit, work done.
            // We would record commit here.
            this.setStateFinished();
            this.storeCommit(this.getTransactionId(), true);
            break;
        default:
            this.illegalStateTransition("commit");
        }
    }


    private void storeCommit(final TransactionId txid, final boolean wasPrepared) throws XAException, IOException {
        try {
            this.transactionStore.commit(this.getTransactionId(), wasPrepared);
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


    private void illegalStateTransition(final String callName) throws XAException {
        final XAException xae = new XAException("Cannot call " + callName + " now.");
        xae.errorCode = XAException.XAER_PROTO;
        throw xae;
    }


    private void checkForPreparedState(final boolean onePhase) throws XAException {
        if (!onePhase) {
            final XAException xae =
                    new XAException("Cannot do 2 phase commit if the transaction has not been prepared.");
            xae.errorCode = XAException.XAER_PROTO;
            throw xae;
        }
    }


    private void doPrePrepare() throws XAException, IOException {
        try {
            this.prePrepare();
        }
        catch (final XAException e) {
            throw e;
        }
        catch (final Throwable e) {
            LOG.warn("PRE-PREPARE FAILED: ", e);
            this.rollback();
            final XAException xae = new XAException("PRE-PREPARE FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(e);
            throw xae;
        }
    }


    @Override
    public void rollback() throws XAException, IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction rollback: " + this.xid);
        }

        switch (this.getState()) {
        case START_STATE:
            // 1 phase rollback no work done.
            this.setStateFinished();
            break;
        case IN_USE_STATE:
            // 1 phase rollback work done.
            this.setStateFinished();
            this.transactionStore.rollback(this.getTransactionId());
            break;
        case PREPARED_STATE:
            // 2 phase rollback work done.
            this.setStateFinished();
            this.transactionStore.rollback(this.getTransactionId());
            break;
        case FINISHED_STATE:
            // failure to commit
            this.transactionStore.rollback(this.getTransactionId());
            break;
        default:
            throw new XAException("Invalid state");
        }

    }


    @Override
    public int prepare() throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction prepare: " + this.xid);
        }

        switch (this.getState()) {
        case START_STATE:
            // No work done.. no commit/rollback needed.
            this.setStateFinished();
            return XAResource.XA_RDONLY;
        case IN_USE_STATE:
            // We would record prepare here.
            this.doPrePrepare();
            this.setState(Transaction.PREPARED_STATE);
            this.transactionStore.prepare(this.getTransactionId());
            return XAResource.XA_OK;
        default:
            this.illegalStateTransition("prepare");
            return XAResource.XA_RDONLY;
        }
    }


    private void setStateFinished() {
        this.setState(Transaction.FINISHED_STATE);
        this.brokerProcessor.removeTransaction(this.xid);
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