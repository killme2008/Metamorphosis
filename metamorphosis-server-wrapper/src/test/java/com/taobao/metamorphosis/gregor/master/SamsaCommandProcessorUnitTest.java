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
package com.taobao.metamorphosis.gregor.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.gregor.Constants;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.ExecutorsManager;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.network.SessionContextHolder;
import com.taobao.metamorphosis.server.network.SessionContextImpl;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metamorphosis.utils.MessageFlagUtils;
import com.taobao.metamorphosis.utils.MessageUtils;


public class SamsaCommandProcessorUnitTest {

    protected MessageStoreManager storeManager;
    protected MetaConfig metaConfig;
    protected Connection conn;
    protected IMocksControl mocksControl;
    protected SamsaCommandProcessor commandProcessor;
    protected StatsManager statsManager;
    protected IdWorker idWorker;
    protected BrokerZooKeeper brokerZooKeeper;
    protected ExecutorsManager executorsManager;
    protected SessionContext sessionContext;
    protected RemotingClient remotingClient;
    final String topic = "SamsaCommandProcessorUnitTest";
    private final String slaveUrl = "meta://localhost:8124";


    protected void mock() {

        this.metaConfig = new MetaConfig();
        this.mocksControl = EasyMock.createControl();
        this.storeManager = this.mocksControl.createMock(MessageStoreManager.class);
        this.conn = this.mocksControl.createMock(Connection.class);
        this.remotingClient = this.mocksControl.createMock(RemotingClient.class);
        this.sessionContext = new SessionContextImpl(null, this.conn);
        EasyMock.expect(this.conn.getAttribute(SessionContextHolder.GLOBAL_SESSION_KEY)).andReturn(this.sessionContext)
        .anyTimes();
        this.statsManager = new StatsManager(new MetaConfig(), null, null);
        this.idWorker = this.mocksControl.createMock(IdWorker.class);
        this.brokerZooKeeper = this.mocksControl.createMock(BrokerZooKeeper.class);
        this.executorsManager = this.mocksControl.createMock(ExecutorsManager.class);
        this.commandProcessor = new SamsaCommandProcessor();
        this.commandProcessor.setMetaConfig(this.metaConfig);
        this.commandProcessor.setStoreManager(this.storeManager);
        this.commandProcessor.setStatsManager(this.statsManager);
        this.commandProcessor.setBrokerZooKeeper(this.brokerZooKeeper);
        this.commandProcessor.setIdWorker(this.idWorker);
        this.commandProcessor.setExecutorsManager(this.executorsManager);
        this.commandProcessor.setRemotingClient(this.remotingClient);
        this.commandProcessor.setSlaveUrl(this.slaveUrl);
    }


    @Before
    public void setUp() {
        this.mock();
        OpaqueGenerator.resetOpaque();
    }


    @Test
    public void testProcessPutRequestNormal() throws Exception {
        final int partition = 1;
        final int opaque = -1;
        final long offset = 1024L;
        final byte[] data = new byte[1024];
        final int flag = MessageFlagUtils.getFlag(null);
        final PutCommand request =
                new PutCommand(this.topic, partition, data, flag, CheckSum.crc32(data), null, opaque);
        final long msgId = 100000L;
        EasyMock.expect(this.remotingClient.isConnected(this.slaveUrl)).andReturn(true);
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.idWorker.nextId()).andReturn(msgId);
        EasyMock.expect(this.storeManager.getOrCreateMessageStore(this.topic, partition)).andReturn(store);
        final BooleanCommand expectResp =
                new BooleanCommand(HttpStatus.Success, msgId + " " + partition + " " + offset, opaque);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final PutCallback cb = new PutCallback() {

            @Override
            public void putComplete(final ResponseCommand resp) {
                invoked.set(true);
                if (!expectResp.equals(resp)) {
                    throw new RuntimeException();
                }
            }
        };
        final SamsaCommandProcessor.SyncAppendCallback apdcb =
                this.commandProcessor.new SyncAppendCallback(partition,
                    this.metaConfig.getBrokerId() + "-" + partition, request, msgId, cb);
        store.append(msgId, request, apdcb);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((SamsaCommandProcessor.SyncAppendCallback) EasyMock.getCurrentArguments()[2]).appendComplete(Location
                    .create(offset, 1024));
                return null;
            }

        });
        this.remotingClient.sendToGroup(this.slaveUrl, new SyncCommand(request.getTopic(), partition,
            request.getData(), request.getFlag(), msgId, CheckSum.crc32(data), OpaqueGenerator.getNextOpaque()), apdcb,
            this.commandProcessor.getSendToSlaveTimeoutInMills(), TimeUnit.MILLISECONDS);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((SingleRequestCallBackListener) EasyMock.getCurrentArguments()[2]).onResponse(new BooleanCommand(
                    HttpStatus.Success, null, OpaqueGenerator.getNextOpaque()), SamsaCommandProcessorUnitTest.this.conn);
                return null;
            }

        });
        this.brokerZooKeeper.registerTopicInZk(this.topic, false);
        EasyMock.expectLastCall();
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        this.commandProcessor.processPutCommand(request, this.sessionContext, cb);
        this.mocksControl.verify();
        assertEquals(0, this.statsManager.getCmdPutFailed());
        // Must be invoked
        assertTrue(invoked.get());
    }


    @Test
    public void testProcessPutRequest_SlaveDisconnected() throws Exception {
        final int partition = 1;
        final int opaque = -1;
        final long offset = 1024L;
        final byte[] data = new byte[1024];
        final int flag = MessageFlagUtils.getFlag(null);
        final PutCommand request = new PutCommand(this.topic, partition, data, null, flag, opaque);
        final long msgId = 100000L;
        // Slave is disconnected
        EasyMock.expect(this.remotingClient.isConnected(this.slaveUrl)).andReturn(false);
        final BooleanCommand expectResp =
                new BooleanCommand(HttpStatus.InternalServerError, "Slave is disconnected ", opaque);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final PutCallback cb = new PutCallback() {

            @Override
            public void putComplete(final ResponseCommand resp) {
                invoked.set(true);
                if (!expectResp.equals(resp)) {
                    throw new RuntimeException();
                }
            }
        };
        this.brokerZooKeeper.registerTopicInZk(this.topic, false);
        EasyMock.expectLastCall();
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        this.commandProcessor.processPutCommand(request, this.sessionContext, cb);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdPutFailed());
        // Must be invoked
        assertTrue(invoked.get());
    }


    @Test
    public void testProcessPutRequestSlaveFailed() throws Exception {
        this.testProcessPutCommandSlaveFailed0();
        this.mocksControl.verify();

    }


    private void testProcessPutCommandSlaveFailed0() throws IOException, NotifyRemotingException, Exception {
        final int partition = 1;
        final int opaque = -1;
        final long offset = 1024L;
        final byte[] data = new byte[1024];
        final int flag = MessageFlagUtils.getFlag(null);
        final PutCommand request =
                new PutCommand(this.topic, partition, data, flag, CheckSum.crc32(data), null, opaque);
        final long msgId = 100000L;
        EasyMock.expect(this.remotingClient.isConnected(this.slaveUrl)).andReturn(true);
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.idWorker.nextId()).andReturn(msgId);
        EasyMock.expect(this.storeManager.getOrCreateMessageStore(this.topic, partition)).andReturn(store);
        final BooleanCommand expectResp =
                new BooleanCommand(
                    HttpStatus.InternalServerError,
                    "Put message to [slave 'meta://localhost:8124'] [partition 'SamsaCommandProcessorUnitTest-1'] failed",
                    opaque);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final PutCallback cb = new PutCallback() {

            @Override
            public void putComplete(final ResponseCommand resp) {
                invoked.set(true);
                System.out.println(((BooleanCommand) resp).getErrorMsg());
                if (!expectResp.equals(resp)) {
                    throw new RuntimeException();
                }
            }
        };
        final SamsaCommandProcessor.SyncAppendCallback apdcb =
                this.commandProcessor.new SyncAppendCallback(partition,
                    this.metaConfig.getBrokerId() + "-" + partition, request, msgId, cb);
        store.append(msgId, request, apdcb);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((SamsaCommandProcessor.SyncAppendCallback) EasyMock.getCurrentArguments()[2]).appendComplete(Location
                    .create(offset, 1024));
                return null;
            }

        });
        this.remotingClient.sendToGroup(this.slaveUrl, new SyncCommand(request.getTopic(), partition,
            request.getData(), request.getFlag(), msgId, CheckSum.crc32(data), OpaqueGenerator.getNextOpaque()), apdcb,
            this.commandProcessor.getSendToSlaveTimeoutInMills(), TimeUnit.MILLISECONDS);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((SingleRequestCallBackListener) EasyMock.getCurrentArguments()[2]).onResponse(new BooleanCommand(
                    HttpStatus.InternalServerError, "Put to slave failed", OpaqueGenerator.getNextOpaque()),
                    SamsaCommandProcessorUnitTest.this.conn);
                return null;
            }

        });
        this.brokerZooKeeper.registerTopicInZk(this.topic, false);
        EasyMock.expectLastCall();
        // EasyMock.expect(this.brokerZooKeeper.getBrokerString()).andReturn("meta://localhost:8123");;
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        this.commandProcessor.processPutCommand(request, this.sessionContext, cb);
        assertEquals(1, this.statsManager.getCmdPutFailed());
        // Must be invoked
        assertTrue(invoked.get());
    }


    @Test
    public void testProcessPutRequestMasterFailed() throws Exception {
        final int partition = 1;
        final int opaque = -1;
        final long offset = -1;
        final byte[] data = new byte[1024];
        final int flag = MessageFlagUtils.getFlag(null);
        final PutCommand request =
                new PutCommand(this.topic, partition, data, flag, CheckSum.crc32(data), null, opaque);
        final long msgId = 100000L;
        EasyMock.expect(this.remotingClient.isConnected(this.slaveUrl)).andReturn(true);
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.idWorker.nextId()).andReturn(msgId);
        EasyMock.expect(this.storeManager.getOrCreateMessageStore(this.topic, partition)).andReturn(store);
        final BooleanCommand expectResp =
                new BooleanCommand(
                    HttpStatus.InternalServerError,
                    "Put message to [master 'meta://localhost:8123'] [partition 'SamsaCommandProcessorUnitTest-1'] failed",
                    opaque);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final PutCallback cb = new PutCallback() {

            @Override
            public void putComplete(final ResponseCommand resp) {
                invoked.set(true);
                System.out.println(((BooleanCommand) resp).getErrorMsg());
                if (!expectResp.equals(resp)) {
                    throw new RuntimeException();
                }
            }
        };
        final SamsaCommandProcessor.SyncAppendCallback apdcb =
                this.commandProcessor.new SyncAppendCallback(partition,
                    this.metaConfig.getBrokerId() + "-" + partition, request, msgId, cb);
        store.append(msgId, request, apdcb);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((SamsaCommandProcessor.SyncAppendCallback) EasyMock.getCurrentArguments()[2]).appendComplete(Location
                    .create(offset, 1024));
                return null;
            }

        });
        this.remotingClient.sendToGroup(this.slaveUrl, new SyncCommand(request.getTopic(), partition,
            request.getData(), request.getFlag(), msgId, CheckSum.crc32(data), OpaqueGenerator.getNextOpaque()), apdcb,
            this.commandProcessor.getSendToSlaveTimeoutInMills(), TimeUnit.MILLISECONDS);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((SingleRequestCallBackListener) EasyMock.getCurrentArguments()[2]).onResponse(new BooleanCommand(
                    HttpStatus.Success, null, OpaqueGenerator.getNextOpaque()), SamsaCommandProcessorUnitTest.this.conn);
                return null;
            }

        });
        this.brokerZooKeeper.registerTopicInZk(this.topic, false);
        EasyMock.expectLastCall();
        EasyMock.expect(this.brokerZooKeeper.getBrokerString()).andReturn("meta://localhost:8123");
        this.mocksControl.replay();
        OpaqueGenerator.resetOpaque();
        this.commandProcessor.processPutCommand(request, this.sessionContext, cb);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdPutFailed());
        // Must be invoked
        assertTrue(invoked.get());
    }


    @Test
    public void testProcessPutRequest_SlaveFailed_HealSlave() throws Exception {
        this.commandProcessor.setSlaveContinuousFailureThreshold(1);
        this.brokerZooKeeper.unregisterEveryThing();
        EasyMock.expectLastCall();
        final long msgId = 100000L;
        EasyMock.expect(this.idWorker.nextId()).andReturn(msgId);
        final String testTopic = Constants.TEST_SLAVE_TOPIC;
        Message msg = new Message(testTopic, "test".getBytes());
        // ·¢Íùslave
        byte[] encodePayload = MessageUtils.encodePayload(msg);
        int flag = 0;
        int partition = 0;
        // skip two messages.
        OpaqueGenerator.getNextOpaque();
        OpaqueGenerator.getNextOpaque();
        // check if slave is ok.
        final BooleanCommand expectResp = new BooleanCommand(HttpStatus.Success, msgId + " " + partition + " " + 0, -1);
        SyncCommand command =
                new SyncCommand(testTopic, partition, encodePayload, flag, msgId, CheckSum.crc32(encodePayload),
                    OpaqueGenerator.getNextOpaque());
        EasyMock.expect(
            this.remotingClient.invokeToGroup(this.slaveUrl, command,
                this.commandProcessor.getSendToSlaveTimeoutInMills(), TimeUnit.MILLISECONDS)).andReturn(expectResp);
        OpaqueGenerator.resetOpaque();

        this.brokerZooKeeper.reRegisterEveryThing();
        EasyMock.expectLastCall();

        assertNull(this.commandProcessor.getHealThread());
        this.testProcessPutCommandSlaveFailed0();
        Thread.sleep(2000);
        this.mocksControl.verify();
        assertNull(this.commandProcessor.getHealThread());
        assertEquals(0, this.commandProcessor.getSlaveContinuousFailures().get());
    }

}