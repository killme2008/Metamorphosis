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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.utils.XIDGenerator;
import com.taobao.metamorphosis.transaction.XATransactionId;


public class XATransactionUnitTest extends TransactionUnitTest {
    private XATransaction tx;
    private CommandProcessor processor;


    @Override
    @Before
    public void setUp() {
        super.setUp();
        this.processor = EasyMock.createMock(CommandProcessor.class);
        this.xid = XIDGenerator.createXID(100, "test");
        this.tx = new XATransaction(this.processor, this.transactionStore, (XATransactionId) this.xid);
        assertEquals("test", this.tx.getUniqueQualifier());
    }


    @Override
    @After
    public void tearDown() {
        super.tearDown();
        EasyMock.verify(this.processor);
    }


    public void replay() {
        EasyMock.replay(this.processor, this.transactionStore);
    }


    @Test
    public void testCommitOnePhase() throws Exception {
        assertFalse(this.tx.isPrepared());
        this.mockRemoveTx();
        this.replay();
        this.tx.commit(true);
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
    }


    private void mockRemoveTx() {
        this.processor.removeTransaction((XATransactionId) this.xid);
        EasyMock.expectLastCall();
    }


    @Test
    public void testCommitTwoPhase() throws Exception {
        this.tx.setTransactionInUse();
        this.replay();
        try {
            this.tx.commit(false);
            fail();
        }
        catch (final XAException e) {
            assertEquals("Cannot do 2 phase commit if the transaction has not been prepared.", e.getMessage());
        }
    }


    @Test
    public void testPrepareNoWorkDone() throws Exception {
        this.mockRemoveTx();
        this.replay();
        assertEquals(XAResource.XA_RDONLY, this.tx.prepare());
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
        assertFalse(this.tx.isPrepared());
    }


    @Test
    public void testPrepareCommitTwoPhase() throws Exception {
        this.tx.setTransactionInUse();
        this.mockStorePrepare();
        this.mockStoreCommitTwoPhase();
        this.mockRemoveTx();
        this.replay();
        assertEquals(XAResource.XA_OK, this.tx.prepare());
        assertEquals(this.tx.getState(), Transaction.PREPARED_STATE);
        assertTrue(this.tx.isPrepared());

        this.tx.commit(false);
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
    }


    @Test
    public void testRollback() throws Exception {
        this.mockRemoveTx();
        this.replay();
        this.tx.rollback();
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
    }


    @Test
    public void testPrepareRollback() throws Exception {
        this.tx.setTransactionInUse();
        this.mockStorePrepare();
        this.mockStoreRollback();
        this.mockRemoveTx();
        this.replay();
        assertEquals(XAResource.XA_OK, this.tx.prepare());
        this.tx.rollback();
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
    }


    @Test
    public void testPrepareCommitRollback() throws Exception {
        this.tx.setTransactionInUse();
        this.mockStorePrepare();
        this.mockStoreCommitTwoPhase();
        this.mockStoreRollback();
        this.mockRemoveTx();
        this.replay();
        assertEquals(XAResource.XA_OK, this.tx.prepare());
        this.tx.commit(false);
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
        this.tx.rollback();
        assertEquals(this.tx.getState(), Transaction.FINISHED_STATE);
    }

}