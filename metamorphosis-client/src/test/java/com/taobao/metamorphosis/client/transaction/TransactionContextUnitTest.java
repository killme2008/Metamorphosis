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
package com.taobao.metamorphosis.client.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.TransactionCommand;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.TransactionInfo;
import com.taobao.metamorphosis.transaction.TransactionInfo.TransactionType;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.IdGenerator;
import com.taobao.metamorphosis.utils.LongSequenceGenerator;


public class TransactionContextUnitTest {
    private static final String UNIQUE_QUALIFIER = XIDGenerator.UNIQUE_QUALIFIER;
    private TransactionContext context;
    private MockSession session;
    private RemotingClient remotingClient;

    private IdGenerator idGenerator;

    private String sessionId;

    private static final long DEFAULT_REQ_TIMEOUT = 5000L;

    static class MockSession implements TransactionSession {
        private final String sessionId;
        private boolean removeCtx = false;


        public MockSession(final String sessionId) {
            super();
            this.sessionId = sessionId;
        }


        @Override
        public void removeContext(final TransactionContext ctx) {
            this.removeCtx = true;

        }


        @Override
        public String getSessionId() {
            return this.sessionId;
        }

    }


    @Before
    public void setUp() {
        this.idGenerator = new IdGenerator();
        this.remotingClient = EasyMock.createMock(RemotingClient.class);
        this.sessionId = this.idGenerator.generateId();
        this.session = new MockSession(this.sessionId);
        this.context =
                new TransactionContext(this.remotingClient, null, this.session, new LongSequenceGenerator(), 0,
                    DEFAULT_REQ_TIMEOUT);
        this.context.setUniqueQualifier(UNIQUE_QUALIFIER);
        OpaqueGenerator.resetOpaque();
    }


    @After
    public void tearDown() {
        this.verify();
    }


    private void verify() {
        EasyMock.verify(this.remotingClient);
    }


    private void replay() {
        EasyMock.replay(this.remotingClient);
    }


    @Test
    public void testLocalBeginCommit() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);
        final TransactionId id = new LocalTransactionId(this.sessionId, 1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.COMMIT_ONE_PHASE),
            null);
        assertNull(this.context.getTransactionId());
        assertFalse(this.context.isInTransaction());
        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.begin();
        final TransactionId xid = this.context.getTransactionId();
        assertNotNull(xid);
        assertTrue(xid.isLocalTransaction());
        assertTrue(this.context.isInLocalTransaction());
        assertFalse(this.context.isInXATransaction());
        assertTrue(this.context.isInTransaction());

        this.context.commit();
        assertNull(this.context.getTransactionId());
        assertFalse(this.context.isInTransaction());
    }


    @Test(expected = XAException.class)
    public void testSetTxTimeoutInvalidValue() throws Exception {
        this.replay();
        this.context.setTransactionTimeout(-1);
    }


    @Test
    public void testSetTxTimeout() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);
        final XATransactionId id = XIDGenerator.createXID(1);
        this.context.setTransactionTimeout(3);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN, null, 3), null);
        // this.mockSend(serverUrl, new TransactionInfo(id, this.sessionId,
        // TransactionType.SET_TIMEOUT, 3));
        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);
    }


    @Test(expected = MetaClientException.class)
    public void testBeginUnConnected() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, false);
        this.context.setServerUrl(serverUrl);
        this.replay();
        this.context.begin();
    }


    @Test(expected = MetaClientException.class)
    public void testCommitUnBeginTx() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.context.setServerUrl(serverUrl);
        this.replay();
        this.context.commit();
    }


    @Test(expected = MetaClientException.class)
    public void testRollbackUnBeginTx() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.context.setServerUrl(serverUrl);
        this.replay();
        this.context.rollback();
    }


    @Test
    public void testLocalBeginRollback() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);
        final TransactionId id = new LocalTransactionId(this.sessionId, 1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.ROLLBACK), null);
        assertNull(this.context.getTransactionId());
        assertFalse(this.context.isInTransaction());
        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.begin();
        final TransactionId xid = this.context.getTransactionId();
        assertNotNull(xid);
        assertTrue(xid.isLocalTransaction());
        assertTrue(this.context.isInLocalTransaction());
        assertFalse(this.context.isInXATransaction());
        assertTrue(this.context.isInTransaction());

        this.context.rollback();
        assertNull(this.context.getTransactionId());
        assertFalse(this.context.isInTransaction());
    }


    @Test
    public void testBeginXAWithServerUrl() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);

        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);

        assertNull(this.context.getTransactionId());
        assertFalse(this.context.isInTransaction());
        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);

        final TransactionId xid = this.context.getTransactionId();
        assertNotNull(xid);
        assertEquals(id, xid);
        assertFalse(xid.isLocalTransaction());
        assertFalse(this.context.isInLocalTransaction());
        assertTrue(this.context.isInXATransaction());
        assertTrue(this.context.isInTransaction());
    }


    @Test
    public void testBeginXAEndPrepareCommit() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);

        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);
        this.mockSend(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.END));
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.PREPARE),
            String.valueOf(XAResource.XA_OK));
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.COMMIT_TWO_PHASE),
            null);

        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);
        assertTrue(this.context.isInXATransaction());
        this.context.end(id, XAResource.TMSUCCESS);
        assertFalse(this.context.isInXATransaction());
        this.context.prepare(id);
        this.context.commit(id, false);
        assertFalse(this.context.isInXATransaction());
        assertTrue(this.session.removeCtx);
    }


    @Test(expected = XAException.class)
    public void testBeginXAPrepare() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);

        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);

        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);
        assertTrue(this.context.isInXATransaction());
        // 没有调用end，直接prepare
        this.context.prepare(id);
        fail();
    }


    @Test(expected = XAException.class)
    public void testBeginXACommit() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);

        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);

        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);
        assertTrue(this.context.isInXATransaction());
        // 没有调用end，直接commit
        this.context.commit(id, false);
        fail();
    }


    @Test
    public void testBeginXARollback() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);

        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.ROLLBACK), null);

        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);
        assertFalse(this.session.removeCtx);
        this.context.rollback(id);
        assertTrue(this.session.removeCtx);
    }


    @Test
    public void testBeginXAEndPrepareRollback() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);

        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.BEGIN), null);
        this.mockSend(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.END));
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.PREPARE),
            String.valueOf(XAResource.XA_OK));
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.ROLLBACK), null);

        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.start(id, XAResource.TMNOFLAGS);
        assertFalse(this.session.removeCtx);
        this.context.end(id, XAResource.TMSUCCESS);
        this.context.prepare(id);
        this.context.rollback(id);
        assertTrue(this.session.removeCtx);
        assertFalse(this.context.isInXATransaction());
    }


    @Test
    public void testCommitUnBeginXA() throws Exception {
        // 这是允许的，可能是recover出来的事务
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);
        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.COMMIT_TWO_PHASE),
            null);
        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.commit(id, false);

    }


    @Test
    public void testPrepareUnBeginXA() throws Exception {
        // 这是允许的，可能是recover出来的事务
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setServerUrl(serverUrl);
        final XATransactionId id = XIDGenerator.createXID(1);
        this.mockInvokeSuccess(serverUrl, new TransactionInfo(id, this.sessionId, TransactionType.PREPARE),
            String.valueOf(XAResource.XA_OK));
        OpaqueGenerator.resetOpaque();
        this.replay();
        this.context.prepare(id);

    }


    @Test
    public void testRecover() throws Exception {
        final String serverURL1 = "meta://localhost:8123";
        final String serverURL2 = "meta://localhost:8124";
        final XATransactionId id1 = XIDGenerator.createXID(1);
        final XATransactionId id2 = XIDGenerator.createXID(2);
        final XATransactionId id3 = XIDGenerator.createXID(3);
        final XATransactionId id4 = XIDGenerator.createXID(4);

        this.mockInvokeSuccess(serverURL1, new TransactionInfo(null, this.sessionId, TransactionType.RECOVER),
            id2.getTransactionKey() + "\r\n" + id3.getTransactionKey());
        this.mockInvokeSuccess(serverURL2, new TransactionInfo(null, this.sessionId, TransactionType.RECOVER),
            id1.getTransactionKey() + "\r\n" + id4.getTransactionKey());
        this.replay();
        OpaqueGenerator.resetOpaque();
        this.context.setXareresourceURLs(new String[] { serverURL1, serverURL2 });
        final Xid[] xids = this.context.recover(XAResource.TMNOFLAGS);
        assertEquals(4, xids.length);
        this.assertContains(xids, id1);
        this.assertContains(xids, id2);
        this.assertContains(xids, id3);
        this.assertContains(xids, id4);
    }


    @Test
    public void testIsSameRM() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.context.setServerUrl(serverUrl);

        final TransactionContext ctx2 =
                new TransactionContext(this.remotingClient, null, this.session, new LongSequenceGenerator(), 0,
                    DEFAULT_REQ_TIMEOUT);
        ctx2.setServerUrl(serverUrl);
        assertTrue(this.context.isSameRM(ctx2));
        this.replay();
    }


    @Test
    public void testToXAException() {
        final XAException e = new XAException();
        e.errorCode = XAException.XA_HEURCOM;
        assertSame(e, this.context.toXAException(e));
        XAException xe = this.context.toXAException(new RuntimeException(e));
        assertEquals(XAException.XA_HEURCOM, xe.errorCode);

        xe = this.context.toXAException(new RuntimeException("test"));
        assertEquals(XAException.XAER_RMFAIL, xe.errorCode);
        assertEquals("test", xe.getCause().getMessage());
        this.replay();
    }


    @Test
    public void testRecoverBlank() throws Exception {
        final String serverUrl = "meta://localhost:8123";
        this.mockIsConnected(serverUrl, true);
        this.context.setXareresourceURLs(new String[] { serverUrl });

        this.mockInvokeSuccess(serverUrl, new TransactionInfo(null, this.sessionId, TransactionType.RECOVER), null);
        this.replay();
        OpaqueGenerator.resetOpaque();
        final Xid[] xids = this.context.recover(XAResource.TMNOFLAGS);
        assertEquals(0, xids.length);
    }


    private void assertContains(final Xid[] a, final Xid key) {
        boolean contains = false;
        for (final Xid e : a) {
            if (e.equals(key)) {
                contains = true;
                break;
            }
        }
        if (!contains) {
            throw new AssertionError();
        }
    }


    private void mockIsConnected(final String serverUrl, final boolean rt) {
        EasyMock.expect(this.remotingClient.isConnected(serverUrl)).andReturn(rt).anyTimes();
    }


    private void mockInvokeSuccess(final String serverUrl, final TransactionInfo info, final String result)
            throws InterruptedException, TimeoutException, NotifyRemotingException {
        info.setUniqueQualifier(UNIQUE_QUALIFIER);
        EasyMock.expect(
            this.remotingClient.invokeToGroup(serverUrl, new TransactionCommand(info, OpaqueGenerator.getNextOpaque()),
                DEFAULT_REQ_TIMEOUT, TimeUnit.MILLISECONDS)).andReturn(
                    new BooleanCommand(HttpStatus.Success, result, 0));
    }


    private void mockSend(final String serverUrl, final TransactionInfo info) throws NotifyRemotingException {
        info.setUniqueQualifier(UNIQUE_QUALIFIER);
        this.remotingClient.sendToGroup(serverUrl, new TransactionCommand(info, OpaqueGenerator.getNextOpaque()),
            TransactionContext.END_XA_TX_LISTENER, 5000, TimeUnit.MILLISECONDS);
        EasyMock.expectLastCall();
    }

}