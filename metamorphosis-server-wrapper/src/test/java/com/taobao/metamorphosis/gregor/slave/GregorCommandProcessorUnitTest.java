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
package com.taobao.metamorphosis.gregor.slave;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor.StoreAppendCallback;
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
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metamorphosis.utils.MessageFlagUtils;


public class GregorCommandProcessorUnitTest {
    protected MessageStoreManager storeManager;
    protected MetaConfig metaConfig;
    protected Connection conn;
    protected IMocksControl mocksControl;
    protected GregorCommandProcessor commandProcessor;
    protected StatsManager statsManager;
    protected IdWorker idWorker;
    protected BrokerZooKeeper brokerZooKeeper;
    protected ExecutorsManager executorsManager;
    protected SessionContext sessionContext;
    protected RemotingClient remotingClient;
    final String topic = "GregorCommandProcessorUnitTest";


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
        this.commandProcessor = new GregorCommandProcessor();
        this.commandProcessor.setMetaConfig(this.metaConfig);
        this.commandProcessor.setStoreManager(this.storeManager);
        this.commandProcessor.setStatsManager(this.statsManager);
        this.commandProcessor.setBrokerZooKeeper(this.brokerZooKeeper);
        this.commandProcessor.setIdWorker(this.idWorker);
        this.commandProcessor.setExecutorsManager(this.executorsManager);
    }


    @Before
    public void setUp() {
        this.mock();
        OpaqueGenerator.resetOpaque();
    }


    @Test
    public void testProcessSyncCommandNormal() throws Exception {
        final int partition = 1;
        final int opaque = -1;
        final long offset = 1024L;
        final byte[] data = new byte[1024];
        final long msgId = 100000L;
        final int flag = MessageFlagUtils.getFlag(null);
        final SyncCommand request = new SyncCommand(this.topic, partition, data, flag, msgId, -1, opaque);

        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
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
        store.append(msgId, request,
            this.commandProcessor.new StoreAppendCallback(partition, this.metaConfig.getBrokerId() + "-" + partition,
                request, msgId, cb));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((StoreAppendCallback) EasyMock.getCurrentArguments()[2]).appendComplete(Location.create(offset, 1024));
                return null;
            }

        });
        this.mocksControl.replay();
        this.commandProcessor.processSyncCommand(request, this.sessionContext, cb);
        this.mocksControl.verify();
        assertEquals(0, this.statsManager.getCmdPutFailed());
        assertTrue(invoked.get());
    }


    @Test
    public void testProcessRequestAppendFail() throws Exception {
        final int partition = 1;
        final int opaque = -1;
        final byte[] data = new byte[1024];
        final int flag = MessageFlagUtils.getFlag(null);
        final long msgId = 100000L;
        final SyncCommand request = new SyncCommand(this.topic, partition, data, flag, msgId, -1, opaque);

        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getOrCreateMessageStore(this.topic, partition)).andReturn(store);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final BooleanCommand expectResp =
                new BooleanCommand(
                    HttpStatus.InternalServerError,
                    "Put message to [broker 'meta://localhost:8123'] [partition 'GregorCommandProcessorUnitTest-1'] failed.",
                    request.getOpaque());
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
        store.append(msgId, request,
            this.commandProcessor.new StoreAppendCallback(partition, this.metaConfig.getBrokerId() + "-" + partition,
                request, msgId, cb));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((StoreAppendCallback) EasyMock.getCurrentArguments()[2]).appendComplete(Location.InvalidLocaltion);
                return null;
            }

        });
        EasyMock.expect(this.brokerZooKeeper.getBrokerString()).andReturn("meta://localhost:8123");
        this.mocksControl.replay();
        this.commandProcessor.processSyncCommand(request, this.sessionContext, cb);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdPutFailed());
        assertTrue(invoked.get());

    }


    @Test
    public void testProcessRequestAppendException() throws Exception {
        final int partition = 1;
        final int opaque = -1;
        final byte[] data = new byte[1024];
        final int flag = MessageFlagUtils.getFlag(null);
        final long msgId = 100000L;
        final SyncCommand request = new SyncCommand(this.topic, partition, data, flag, msgId, -1, opaque);

        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getOrCreateMessageStore(this.topic, partition)).andReturn(store);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final BooleanCommand expectResp =
                new BooleanCommand(
                    HttpStatus.InternalServerError,
                    "Put message to [broker 'meta://localhost:8123'] [partition 'GregorCommandProcessorUnitTest-1'] failed.Detail:Mock exception",
                    request.getOpaque());
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
        store.append(msgId, request,
            this.commandProcessor.new StoreAppendCallback(partition, this.metaConfig.getBrokerId() + "-" + partition,
                request, msgId, cb));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                throw new RuntimeException("Mock exception");
            }

        });
        EasyMock.expect(this.brokerZooKeeper.getBrokerString()).andReturn("meta://localhost:8123");
        this.mocksControl.replay();
        this.commandProcessor.processSyncCommand(request, this.sessionContext, cb);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdPutFailed());
        assertTrue(invoked.get());

    }
}