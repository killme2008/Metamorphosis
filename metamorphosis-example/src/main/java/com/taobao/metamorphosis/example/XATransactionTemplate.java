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
package com.taobao.metamorphosis.example;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import com.taobao.metamorphosis.client.producer.XAMessageProducer;
import com.taobao.metamorphosis.example.XACallback.Status;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * XA事务模板，用于简化使用XA事务处理数据库操作和meta发送消息。
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-29
 * 
 */
public class XATransactionTemplate {

    private XADataSource xaDataSource;

    private XAMessageProducer xaMessageProducer;

    private TransactionManager transactionManager;

    private int transactionTimeout;


    public XATransactionTemplate() {
        super();
    }


    public XATransactionTemplate(final TransactionManager transactionManager, final XADataSource xaDataSource,
            final XAMessageProducer xaMessageProducer) {
        super();
        this.xaDataSource = xaDataSource;
        this.xaMessageProducer = xaMessageProducer;
        this.transactionManager = transactionManager;
    }


    public TransactionManager getTransactionManager() {
        return this.transactionManager;
    }


    public void setTransactionManager(final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }


    private void init() throws SystemException {
        if (this.getTransactionManager() == null) {
            throw new IllegalArgumentException("null tm");
        }
        if (this.getXaDataSource() == null) {
            throw new IllegalArgumentException("null XADatasource");
        }
        if (this.getXAMessageProducer() == null) {
            throw new IllegalArgumentException("null XAMessageProducer");
        }
        this.transactionManager.setTransactionTimeout(this.transactionTimeout);
    }

    static interface WrapExecutor {
        public Object run() throws Exception;
    }


    private XAConnection getXAConnection() throws SQLException {
        final XADataSource xads = this.getXaDataSource();
        if (xads == null) {
            throw new IllegalArgumentException("Null xaDataSource");
        }
        return xads.getXAConnection();
    }


    public Object executeCallback(final XACallback callback) {
        XAMessageProducer producer = null;
        XAConnection conn = null;
        Transaction tx = null;
        Connection sqlConn = null;
        try {
            this.init();
            producer = this.getXAMessageProducer();
            conn = this.getXAConnection();

            tx = this.beginTx(producer, conn);
            sqlConn = conn.getConnection();
            Status status = new Status();
            final Object rt = callback.execute(sqlConn, producer, status);
            this.commitOrRollbackTx(producer, conn, tx, status.rollback);
            return rt;
        }
        catch (final Exception e) {
            try {
                this.commitOrRollbackTx(producer, conn, tx, true);
            }
            catch (final Exception ex) {
                throw new XAWrapException(ex);
            }
            throw new XAWrapException("Execute xa transaction callback error", e);
        }
        finally {
            if (sqlConn != null) {
                try {
                    sqlConn.close();
                }
                catch (final Exception e) {
                    throw new XAWrapException("Close jdbc connection failed", e);
                }
            }
        }

    }


    private Transaction beginTx(final XAMessageProducer producer, final XAConnection conn) throws SystemException,
    MetaClientException, SQLException, RollbackException, NotSupportedException {
        this.transactionManager.begin();
        final Transaction tx = this.transactionManager.getTransaction();
        if (tx == null) {
            throw new IllegalStateException("Could not get transaction from tm");
        }
        final XAResource metaXares = producer.getXAResource();
        final XAResource jdbcXares = conn.getXAResource();
        tx.enlistResource(metaXares);
        tx.enlistResource(jdbcXares);
        return tx;
    }


    private void commitOrRollbackTx(final XAMessageProducer producer, final XAConnection conn, final Transaction tx,
            final boolean error) throws Exception {

        if (tx == null) {
            return;
        }

        int flag = XAResource.TMSUCCESS;
        final XAResource metaXares = producer.getXAResource();
        final XAResource jdbcXares = conn.getXAResource();

        if (error) {
            flag = XAResource.TMFAIL;
        }
        tx.delistResource(metaXares, flag);
        tx.delistResource(jdbcXares, flag);

        if (error) {
            this.getTransactionManager().rollback();
        }
        else {
            this.getTransactionManager().commit();
        }
    }


    public int getTransactionTimeout() {
        return this.transactionTimeout;
    }


    public void setTransactionTimeout(final int transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }


    public XAMessageProducer getXAMessageProducer() {
        return this.xaMessageProducer;
    }


    public XADataSource getXaDataSource() {
        return this.xaDataSource;
    }


    public void setXaDataSource(final XADataSource xaDataSource) {
        this.xaDataSource = xaDataSource;
    }
}