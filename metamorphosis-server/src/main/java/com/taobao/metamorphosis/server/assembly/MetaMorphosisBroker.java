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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.kernel.HeartBeatRequestCommand;
import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.gecko.service.config.ServerConfig;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.network.TransactionCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.network.GetProcessor;
import com.taobao.metamorphosis.server.network.OffsetProcessor;
import com.taobao.metamorphosis.server.network.PutProcessor;
import com.taobao.metamorphosis.server.network.QuitProcessor;
import com.taobao.metamorphosis.server.network.StatsProcessor;
import com.taobao.metamorphosis.server.network.TransactionProcessor;
import com.taobao.metamorphosis.server.network.VersionProcessor;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.DeletePolicy;
import com.taobao.metamorphosis.server.store.DeletePolicyFactory;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.MetaMBeanServer;
import com.taobao.metamorphosis.utils.IdWorker;


;

/**
 * 组装的meta server
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public class MetaMorphosisBroker implements MetaMorphosisBrokerMBean {
    private final class ShutdownHook extends Thread {
        @Override
        public void run() {
            MetaMorphosisBroker.this.runShutdownHook = true;
            MetaMorphosisBroker.this.stop();
        }
    }

    private final MessageStoreManager storeManager;
    private final ExecutorsManager executorsManager;
    private final StatsManager statsManager;
    private final RemotingServer remotingServer;
    private final MetaConfig metaConfig;
    private final IdWorker idWorker;
    private final BrokerZooKeeper brokerZooKeeper;
    private final ConsumerFilterManager consumerFilterManager;

    private CommandProcessor brokerProcessor;
    static final Log log = LogFactory.getLog(MetaMorphosisBroker.class);
    private boolean shutdown = true;
    private volatile boolean runShutdownHook = false;
    private final ShutdownHook shutdownHook;
    private boolean registerZkSuccess;


    public CommandProcessor getBrokerProcessor() {
        return this.brokerProcessor;
    }


    public MetaConfig getMetaConfig() {
        return this.metaConfig;
    }


    public synchronized boolean isShutdown() {
        return this.shutdown;
    }


    public MessageStoreManager getStoreManager() {
        return this.storeManager;
    }


    public ExecutorsManager getExecutorsManager() {
        return this.executorsManager;
    }


    public StatsManager getStatsManager() {
        return this.statsManager;
    }


    public RemotingServer getRemotingServer() {
        return this.remotingServer;
    }


    public ConsumerFilterManager getConsumerFilterManager() {
        return this.consumerFilterManager;
    }


    public IdWorker getIdWorker() {
        return this.idWorker;
    }


    public BrokerZooKeeper getBrokerZooKeeper() {
        return this.brokerZooKeeper;
    }


    public void setBrokerProcessor(final CommandProcessor brokerProcessor) {
        this.brokerProcessor = brokerProcessor;
    }


    public MetaMorphosisBroker(final MetaConfig metaConfig) {
        super();
        this.shutdownHook = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);
        this.metaConfig = metaConfig;
        this.remotingServer = newRemotingServer(metaConfig);
        this.executorsManager = new ExecutorsManager(metaConfig);
        this.idWorker = new IdWorker(metaConfig.getBrokerId());
        this.storeManager = new MessageStoreManager(metaConfig, this.newDeletePolicy(metaConfig));
        this.statsManager = new StatsManager(this.metaConfig, this.storeManager, this.remotingServer);
        this.brokerZooKeeper = new BrokerZooKeeper(metaConfig);
        JournalTransactionStore transactionStore = null;
        try {
            transactionStore = new JournalTransactionStore(metaConfig.getDataLogPath(), this.storeManager, metaConfig);
        }
        catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Initializing transaction store failed", e);
        }
        try {
            this.consumerFilterManager = new ConsumerFilterManager(metaConfig);
        }
        catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Initializing ConsumerFilterManager failed", e);
        }
        final BrokerCommandProcessor next =
                new BrokerCommandProcessor(this.storeManager, this.executorsManager, this.statsManager,
                    this.remotingServer, metaConfig, this.idWorker, this.brokerZooKeeper, this.consumerFilterManager);
        this.brokerProcessor =
                new TransactionalCommandProcessor(metaConfig, this.storeManager, this.idWorker, next, transactionStore,
                    this.statsManager);
        MetaMBeanServer.registMBean(this, null);
    }


    private DeletePolicy newDeletePolicy(final MetaConfig metaConfig) {
        final String deletePolicy = metaConfig.getDeletePolicy();
        if (deletePolicy != null) {
            return DeletePolicyFactory.getDeletePolicy(deletePolicy);
        }
        return null;
    }


    private static RemotingServer newRemotingServer(final MetaConfig metaConfig) {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.setWireFormatType(new MetamorphosisWireFormatType());
        serverConfig.setPort(metaConfig.getServerPort());
        final RemotingServer server = RemotingFactory.newRemotingServer(serverConfig);
        return server;
    }


    public synchronized void start() {
        if (!this.shutdown) {
            return;
        }
        this.shutdown = false;
        this.storeManager.init();
        this.executorsManager.init();
        this.statsManager.init();
        this.registerProcessors();
        try {
            this.remotingServer.start();
        }
        catch (final NotifyRemotingException e) {
            throw new MetamorphosisServerStartupException("start remoting server failed", e);
        }
        try {
            this.brokerZooKeeper.registerBrokerInZk();
            this.brokerZooKeeper.registerMasterConfigFileChecksumInZk();
            this.addTopicsChangeListener();
            this.registerTopicsInZk();
            this.registerZkSuccess = true;
        }
        catch (final Exception e) {
            this.registerZkSuccess = false;
            throw new MetamorphosisServerStartupException("Register broker to zk failed", e);
        }
        log.info("Starting metamorphosis server...");
        this.brokerProcessor.init();
        log.info("Start metamorphosis server successfully");
    }


    private void registerProcessors() {
        this.remotingServer.registerProcessor(GetCommand.class, new GetProcessor(this.brokerProcessor,
            this.executorsManager.getGetExecutor()));
        this.remotingServer.registerProcessor(PutCommand.class, new PutProcessor(this.brokerProcessor,
            this.executorsManager.getUnOrderedPutExecutor()));
        this.remotingServer.registerProcessor(OffsetCommand.class, new OffsetProcessor(this.brokerProcessor,
            this.executorsManager.getGetExecutor()));
        this.remotingServer
        .registerProcessor(HeartBeatRequestCommand.class, new VersionProcessor(this.brokerProcessor));
        this.remotingServer.registerProcessor(QuitCommand.class, new QuitProcessor(this.brokerProcessor));
        this.remotingServer.registerProcessor(StatsCommand.class, new StatsProcessor(this.brokerProcessor));
        this.remotingServer.registerProcessor(TransactionCommand.class, new TransactionProcessor(this.brokerProcessor,
            this.executorsManager.getUnOrderedPutExecutor()));
    }


    private void addTopicsChangeListener() {
        // 监听topics列表变化并注册到zk
        this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {

            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                try {
                    MetaMorphosisBroker.this.registerTopicsInZk();
                }
                catch (final Exception e) {
                    log.error("Register topic in zk failed", e);
                }
            }
        });
    }


    private void registerTopicsInZk() throws Exception {
        // 先注册配置的topic到zookeeper
        for (final String topic : this.metaConfig.getTopics()) {
            this.brokerZooKeeper.registerTopicInZk(topic, true);
        }
        // 注册加载的topic到zookeeper
        for (final String topic : this.storeManager.getMessageStores().keySet()) {
            this.brokerZooKeeper.registerTopicInZk(topic, true);
        }
    }


    @Override
    public synchronized void stop() {
        if (this.shutdown) {
            return;
        }
        log.info("Stopping metamorphosis server...");
        this.shutdown = true;
        this.brokerZooKeeper.close(this.registerZkSuccess);
        try {
            // Waiting for zookeeper to notify clients.
            Thread.sleep(this.brokerZooKeeper.getZkConfig().zkSyncTimeMs);
        }
        catch (InterruptedException e) {
            // ignore
        }
        this.executorsManager.dispose();
        this.storeManager.dispose();
        this.statsManager.dispose();
        try {
            this.remotingServer.stop();
        }
        catch (final NotifyRemotingException e) {
            log.error("Shutdown remoting server failed", e);
        }

        if (!this.runShutdownHook && this.shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
            }
            catch (Exception e) {
                // ignore
            }
        }

        this.brokerProcessor.dispose();

        EmbedZookeeperServer.getInstance().stop();

        log.info("Stop metamorphosis server successfully");

    }

}