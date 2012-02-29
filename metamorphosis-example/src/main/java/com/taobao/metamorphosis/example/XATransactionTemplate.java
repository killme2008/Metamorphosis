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
import java.util.HashSet;
import java.util.Set;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.RoundRobinPartitionSelector;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;
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

    private XAMessageSessionFactory xaMessageSessionFactory;

    private XAMessageProducer xaMessageProducer;

    private PartitionSelector partitionSelector;

    private TransactionManager transactionManager;

    private int transactionTimeout;

    private Set<String> publishTopics = new HashSet<String>();

    private boolean wasInit = false;


    public XATransactionTemplate() {
        super();
    }


    public XATransactionTemplate(final TransactionManager transactionManager, final XADataSource xaDataSource,
            final XAMessageSessionFactory xaMessageSessionFactory) {
        super();
        this.xaDataSource = xaDataSource;
        this.xaMessageSessionFactory = xaMessageSessionFactory;
        this.transactionManager = transactionManager;
    }


    public TransactionManager getTransactionManager() {
        return this.transactionManager;
    }


    public void publishTopic(final String topic) {
        try {
            this.init();
            final XAMessageProducer producer = this.getXAMessageProducer();
            producer.publish(topic);
            this.publishTopics.add(topic);
        }
        catch (final Exception e) {
            throw new XAWrapException(e);
        }
    }


    public Set<String> getPublishTopics() {
        return this.publishTopics;
    }


    public void setPublishTopics(final Set<String> publishTopics) {
        this.publishTopics = publishTopics;
    }


    public void setTransactionManager(final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }


    private synchronized void init() throws SystemException {
        if (this.wasInit) {
            return;
        }
        this.wasInit = true;
        if (this.getTransactionManager() == null) {
            throw new IllegalArgumentException("null tm");
        }
        if (this.getXaDataSource() == null) {
            throw new IllegalArgumentException("null XADatasource");
        }
        if (this.getXaMessageSessionFactory() == null) {
            throw new IllegalArgumentException("null XAMessageSessionFactory");
        }
        final XAMessageProducer producer = this.getXAMessageProducer();
        for (final String topic : this.publishTopics) {
            producer.publish(topic);
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
            final Object rt = callback.execute(sqlConn, producer);
            this.commitOrRollbackTx(producer, conn, tx, false);
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


    private synchronized XAMessageProducer getXAMessageProducer() {
        if (this.xaMessageProducer != null) {
            return this.xaMessageProducer;
        }
        final XAMessageSessionFactory xasf = this.getXaMessageSessionFactory();
        PartitionSelector ps = this.getPartitionSelector();
        if (ps == null) {
            ps = new RoundRobinPartitionSelector();
        }
        this.xaMessageProducer = xasf.createXAProducer(ps);
        return this.xaMessageProducer;
    }


    public PartitionSelector getPartitionSelector() {
        return this.partitionSelector;
    }


    public void setPartitionSelector(final PartitionSelector partitionSelector) {
        this.partitionSelector = partitionSelector;
    }


    public XADataSource getXaDataSource() {
        return this.xaDataSource;
    }


    public void setXaDataSource(final XADataSource xaDataSource) {
        this.xaDataSource = xaDataSource;
    }


    public XAMessageSessionFactory getXaMessageSessionFactory() {
        return this.xaMessageSessionFactory;
    }


    public void setXaMessageSessionFactory(final XAMessageSessionFactory xaMessageSessionFactory) {
        this.xaMessageSessionFactory = xaMessageSessionFactory;
    }

}