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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.transaction.xa.XAException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.network.SessionContextImpl;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.transaction.BaseTransactionUnitTest;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.server.transaction.XATransaction;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.XIDGenerator;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.utils.IdWorker;


public class TransactionalCommandProcessorUnitTest extends BaseTransactionUnitTest {
    private TransactionalCommandProcessor processor;
    private CommandProcessor next;
    private Connection conn;


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.next = EasyMock.createMock(CommandProcessor.class);
        this.conn = EasyMock.createMock(Connection.class);
        this.next.init();
        EasyMock.expectLastCall().anyTimes();
        this.next.dispose();
        EasyMock.expectLastCall().anyTimes();
        this.newProcessor();
        this.clearHeuristicTxJournal();
    }


    private void newProcessor() throws Exception {
        final MetaConfig metaConfig = new MetaConfig();
        this.processor =
                new TransactionalCommandProcessor(metaConfig, this.messageStoreManager, new IdWorker(0), this.next,
                    this.transactionStore, new StatsManager(new MetaConfig(), this.messageStoreManager, null));
    }


    private void clearHeuristicTxJournal() throws Exception {
        this.processor.getHeuristicTransactionJournal().write(new LinkedHashMap<TransactionId, XATransaction>());
    }


    private void replay() {
        EasyMock.replay(this.next, this.conn);
        this.processor.init();
    }


    @Test
    public void testBeginPutCommitOnePhase() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = new LocalTransactionId("test", 100);
        assertNull(context.getTransactions().get(xid));
        this.processor.beginTransaction(context, xid, 0);
        final Transaction tx = context.getTransactions().get(xid);
        assertNotNull(tx);
        assertSame(xid, tx.getTransactionId());
        this.replay();

        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
        final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        // commit one phase
        this.processor.commitTransaction(context, xid, true);
        store.flush();
        assertEquals(Transaction.FINISHED_STATE, tx.getState());
        assertNull(context.getTransactions().get(xid));

        assertTrue(store.getSizeInBytes() > 0);
    }


    @Test
    public void testSetTransactionTimeout() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = XIDGenerator.createXID(100);
        assertNull(context.getTransactions().get(xid));
        // 设置事务超时
        // this.processor.setTransactionTimeout(context, xid, 3);
        this.replay();
        this.processor.beginTransaction(context, xid, 3);

        final Transaction tx = this.processor.getTransaction(context, xid);
        assertNotNull(tx);
        assertSame(xid, tx.getTransactionId());

        assertNotNull(tx.getTimeoutRef());
        assertFalse(tx.getTimeoutRef().isExpired());

        Thread.sleep(4000);
        assertTrue(tx.getTimeoutRef().isExpired());
        // It's rolled back
        try {
            assertNull(this.processor.getTransaction(context, xid));
            fail();
        }
        catch (final XAException e) {
            assertEquals("XA transaction '" + xid + "' has not been started.", e.getMessage());
        }
    }


    @Test
    public void testBeginPreparedCommitTwoPhase() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = XIDGenerator.createXID(100);
        assertNull(context.getTransactions().get(xid));
        this.processor.beginTransaction(context, xid, 0);
        final Transaction tx = this.processor.getTransaction(context, xid);
        assertNotNull(tx);
        assertSame(xid, tx.getTransactionId());
        this.replay();

        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
        final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        this.processor.prepareTransaction(context, xid);
        assertSame(tx, this.processor.getTransaction(context, xid));

        // commit two phase
        this.processor.commitTransaction(context, xid, false);
        store.flush();
        assertEquals(Transaction.FINISHED_STATE, tx.getState());
        assertNull(context.getTransactions().get(xid));

        assertTrue(store.getSizeInBytes() > 0);
    }


    @Test(expected = XAException.class)
    public void testPutNotBeginTransaction() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = XIDGenerator.createXID(100);
        this.replay();
        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
    }


    @Test
    public void testBeginPutPutCloseRecoverGetPreparedTransactionsCommit() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = XIDGenerator.createXID(100);
        assertNull(context.getTransactions().get(xid));
        this.processor.beginTransaction(context, xid, 0);
        this.replay();

        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
        this.processor.processPutCommand(new PutCommand("topic1", 2, "world".getBytes(), xid, 0, 1), context, null);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        assertEquals(0, this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER).length);

        // prepare
        this.processor.prepareTransaction(context, xid);
        assertEquals(1, this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER).length);
        assertEquals(1, this.processor.getPreparedTransactions(context, null).length);
        assertEquals(0, this.processor.getPreparedTransactions(context, "unknown").length);
        assertSame(this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER)[0], this.processor
            .getTransaction(context, xid).getTransactionId());

        // close and reopen it
        this.tearDown();
        this.init(this.path);
        this.newProcessor();
        this.processor.recoverPreparedTransactions();

        assertEquals(1, this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER).length);
        assertEquals(1, this.processor.getPreparedTransactions(context, null).length);
        assertEquals(0, this.processor.getPreparedTransactions(context, "unknown").length);
        assertSame(this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER)[0], this.processor
            .getTransaction(context, xid).getTransactionId());
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        // commit two phase
        this.processor.commitTransaction(context, xid, false);
        store.flush();
        assertEquals(0, this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER).length);
        assertNull(context.getTransactions().get(xid));

        store.flush();
        assertTrue(store.getSizeInBytes() > 0);

    }


    @Test
    public void testBeginPutRollback() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = new LocalTransactionId("test", 100);
        assertNull(context.getTransactions().get(xid));
        this.processor.beginTransaction(context, xid, 0);
        final Transaction tx = context.getTransactions().get(xid);
        assertNotNull(tx);
        assertSame(xid, tx.getTransactionId());
        this.replay();

        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
        final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        // rollback
        this.processor.rollbackTransaction(context, xid);
        store.flush();
        assertEquals(Transaction.FINISHED_STATE, tx.getState());
        assertNull(context.getTransactions().get(xid));
        store.flush();
        assertEquals(0, store.getSizeInBytes());
    }


    @Test
    public void testBeginPutPrepareRollbackCloseRecover() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = XIDGenerator.createXID(100);
        assertNull(context.getTransactions().get(xid));
        this.processor.beginTransaction(context, xid, 0);
        final Transaction tx = this.processor.getTransaction(context, xid);
        assertNotNull(tx);
        assertSame(xid, tx.getTransactionId());
        this.replay();

        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        // prepare
        this.processor.prepareTransaction(context, xid);
        assertSame(tx, this.processor.getTransaction(context, xid));

        // rollback
        this.processor.rollbackTransaction(context, xid);
        store.flush();
        assertEquals(Transaction.FINISHED_STATE, tx.getState());
        assertNull(context.getTransactions().get(xid));

        assertEquals(0, store.getSizeInBytes());

        // close and reopen it
        this.tearDown();
        this.init(this.path);
        this.newProcessor();
        this.processor.recoverPreparedTransactions();

        // 确认prepare为空
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());
        assertEquals(0, this.processor.getPreparedTransactions(context, XIDGenerator.UNIQUE_QUALIFIER).length);
    }


    @Test
    public void testCommitTransactionHeuristicallyRecoverHeuristicTransactions() throws Exception {
        final SessionContext context = new SessionContextImpl("test", this.conn);
        final TransactionId xid = XIDGenerator.createXID(100);
        assertNull(context.getTransactions().get(xid));
        this.processor.beginTransaction(context, xid, 0);
        final Transaction tx = this.processor.getTransaction(context, xid);
        assertNotNull(tx);
        assertSame(xid, tx.getTransactionId());
        this.replay();

        this.processor.processPutCommand(new PutCommand("topic1", 2, "hello".getBytes(), xid, 0, 1), context, null);
        final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertEquals(0, store.getSizeInBytes());

        // prepare
        this.processor.prepareTransaction(context, xid);
        assertSame(tx, this.processor.getTransaction(context, xid));

        // 手工提交
        this.processor.commitTransactionHeuristically(xid.getTransactionKey(), false);
        Map<TransactionId, XATransaction> heuristicTxMap = this.processor.getXAHeuristicTransactions();
        assertFalse(heuristicTxMap.isEmpty());
        assertTrue(heuristicTxMap.containsKey(xid));
        assertSame(tx, heuristicTxMap.get(xid));
        assertEquals(Transaction.HEURISTIC_COMMIT_STATE, heuristicTxMap.get(xid).getState());

        // 关闭，重新打开recover
        this.processor.dispose();
        this.newProcessor();
        this.processor.recoverHeuristicTransactions();
        // 确认正确recover
        heuristicTxMap = this.processor.getXAHeuristicTransactions();
        assertFalse(heuristicTxMap.isEmpty());
        assertTrue(heuristicTxMap.containsKey(xid));
        assertEquals(tx.getTransactionId(), heuristicTxMap.get(xid).getTransactionId());
        assertEquals(Transaction.HEURISTIC_COMMIT_STATE, heuristicTxMap.get(xid).getState());
    }


    @Override
    @After
    public void tearDown() {
        super.tearDown();
        this.processor.dispose();
        EasyMock.verify(this.next, this.conn);
    }
}