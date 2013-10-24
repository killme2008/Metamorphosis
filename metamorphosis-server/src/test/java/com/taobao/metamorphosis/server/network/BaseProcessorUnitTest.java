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
package com.taobao.metamorphosis.server.network;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.ExecutorsManager;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.IdWorker;


public abstract class BaseProcessorUnitTest {

    protected MessageStoreManager storeManager;
    protected MetaConfig metaConfig;
    protected Connection conn;
    protected IMocksControl mocksControl;
    protected BrokerCommandProcessor commandProcessor;
    protected StatsManager statsManager;
    protected IdWorker idWorker;
    protected BrokerZooKeeper brokerZooKeeper;
    protected ExecutorsManager executorsManager;
    protected SessionContext sessionContext;
    protected ConsumerFilterManager consumerFilterManager;


    protected void mock() {

        this.metaConfig = new MetaConfig();
        this.mocksControl = EasyMock.createControl();
        this.storeManager = this.mocksControl.createMock(MessageStoreManager.class);
        this.conn = this.mocksControl.createMock(Connection.class);
        try {
            this.consumerFilterManager = new ConsumerFilterManager(this.metaConfig);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.sessionContext = new SessionContextImpl(null, this.conn);
        EasyMock.expect(this.conn.getAttribute(SessionContextHolder.GLOBAL_SESSION_KEY)).andReturn(this.sessionContext)
        .anyTimes();
        this.statsManager = new StatsManager(new MetaConfig(), null, null);
        this.idWorker = this.mocksControl.createMock(IdWorker.class);
        this.brokerZooKeeper = this.mocksControl.createMock(BrokerZooKeeper.class);
        this.executorsManager = this.mocksControl.createMock(ExecutorsManager.class);
        this.commandProcessor = new BrokerCommandProcessor();
        this.commandProcessor.setMetaConfig(this.metaConfig);
        this.commandProcessor.setStoreManager(this.storeManager);
        this.commandProcessor.setStatsManager(this.statsManager);
        this.commandProcessor.setBrokerZooKeeper(this.brokerZooKeeper);
        this.commandProcessor.setIdWorker(this.idWorker);
        this.commandProcessor.setExecutorsManager(this.executorsManager);
        this.commandProcessor.setConsumerFilterManager(this.consumerFilterManager);
    }

}