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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.command.ResponseStatus;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.gregor.Constants;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.ExecutorsManager;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.AppendCallback;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * Master的broker command processor，暂不支持所有事务操作
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class SamsaCommandProcessor extends BrokerCommandProcessor {

    private final AtomicInteger slaveContinuousFailures = new AtomicInteger(0);

    private long sendToSlaveTimeoutInMills = 2000;

    private long checkSlaveIntervalInMills = 100;

    private int slaveContinuousFailureThreshold = 100;

    private volatile Thread healThread;


    long getSendToSlaveTimeoutInMills() {
        return this.sendToSlaveTimeoutInMills;
    }


    void setSendToSlaveTimeoutInMills(long sendToSlaveTimeoutInMills) {
        this.sendToSlaveTimeoutInMills = sendToSlaveTimeoutInMills;
    }


    long getCheckSlaveIntervalInMills() {
        return this.checkSlaveIntervalInMills;
    }


    void setCheckSlaveIntervalInMills(long checkSlaveIntervalInMills) {
        this.checkSlaveIntervalInMills = checkSlaveIntervalInMills;
    }


    int getSlaveContinuousFailureThreshold() {
        return this.slaveContinuousFailureThreshold;
    }


    void setSlaveContinuousFailureThreshold(int slaveContinuousFailureThreshold) {
        this.slaveContinuousFailureThreshold = slaveContinuousFailureThreshold;
    }


    Thread getHealThread() {
        return this.healThread;
    }


    void setHealThread(Thread healThread) {
        this.healThread = healThread;
    }


    AtomicInteger getSlaveContinuousFailures() {
        return this.slaveContinuousFailures;
    }


    private void removeMasterTemporaryAndTryToHeal() {
        if (this.healThread != null) {
            return;
        }
        this.brokerZooKeeper.unregisterEveryThing();
        this.healThread = new Thread() {
            @Override
            public void run() {
                boolean slaveOk = false;
                while (!slaveOk && !Thread.currentThread().isInterrupted()) {
                    try {
                        final String testTopic = Constants.TEST_SLAVE_TOPIC;
                        Message msg = new Message(testTopic, "test".getBytes());
                        // 发往slave
                        byte[] encodePayload = MessageUtils.encodePayload(msg);
                        int flag = 0;
                        int partition = 0;
                        SyncCommand command =
                                new SyncCommand(testTopic, partition, encodePayload, flag,
                                    SamsaCommandProcessor.this.idWorker.nextId(), CheckSum.crc32(encodePayload),
                                    OpaqueGenerator.getNextOpaque());
                        ResponseCommand resp =
                                SamsaCommandProcessor.this.remotingClient.invokeToGroup(
                                    SamsaCommandProcessor.this.slaveUrl, command,
                                    SamsaCommandProcessor.this.sendToSlaveTimeoutInMills, TimeUnit.MILLISECONDS);
                        // Slave was back.
                        if (resp.getResponseStatus() == ResponseStatus.NO_ERROR) {
                            slaveOk = true;
                        }
                        else {
                            // Wait some time to re-check it.
                            Thread.sleep(SamsaCommandProcessor.this.checkSlaveIntervalInMills);
                        }
                    }
                    catch (InterruptedException e) {
                        // ignore
                    }
                    catch (Exception e) {
                        log.error("Try send test message to slave failed", e);
                    }
                }
                if (slaveOk) {
                    log.warn("Slave " + SamsaCommandProcessor.this.slaveUrl + " is back.");
                    SamsaCommandProcessor.this.slaveContinuousFailures.set(0);
                    try {
                        SamsaCommandProcessor.this.brokerZooKeeper.reRegisterEveryThing();
                        SamsaCommandProcessor.this.healThread = null;
                    }
                    catch (Exception e) {
                        log.error("Could not register master to zookeeper,please try to restart it.", e);
                    }
                }
                else {
                    log.warn("Check slave status was terminated,and the master would not be re-registered to zookeeper,please check your master/slave and try to restart them properly.");
                }

            }
        };
        this.healThread.setDaemon(true);
        this.healThread.start();
    }

    /**
     * append到message store的callback
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-12-7
     * 
     */
    public final class SyncAppendCallback implements AppendCallback, SingleRequestCallBackListener {
        private final int partition;
        private final String partitionString;
        private final PutCommand request;
        private final long messageId;
        private final PutCallback cb;
        private int respCount; // 应答次数
        private boolean masterSuccess; // master是否成功
        private boolean slaveSuccess; // slave是否成功
        private long appendOffset = -1L;; // 添加到master的offset


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.getOuterType().hashCode();
            result = prime * result + (int) (this.appendOffset ^ this.appendOffset >>> 32);
            result = prime * result + (this.cb == null ? 0 : this.cb.hashCode());
            result = prime * result + (int) (this.messageId ^ this.messageId >>> 32);
            result = prime * result + this.partition;
            result = prime * result + (this.partitionString == null ? 0 : this.partitionString.hashCode());
            result = prime * result + (this.request == null ? 0 : this.request.hashCode());
            return result;
        }


        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (this.getClass() != obj.getClass()) {
                return false;
            }
            final SyncAppendCallback other = (SyncAppendCallback) obj;
            if (!this.getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (this.appendOffset != other.appendOffset) {
                return false;
            }
            if (this.cb == null) {
                if (other.cb != null) {
                    return false;
                }
            }
            else if (!this.cb.equals(other.cb)) {
                return false;
            }
            if (this.messageId != other.messageId) {
                return false;
            }
            if (this.partition != other.partition) {
                return false;
            }
            if (this.partitionString == null) {
                if (other.partitionString != null) {
                    return false;
                }
            }
            else if (!this.partitionString.equals(other.partitionString)) {
                return false;
            }
            if (this.request == null) {
                if (other.request != null) {
                    return false;
                }
            }
            else if (!this.request.equals(other.request)) {
                return false;
            }

            return true;
        }


        public SyncAppendCallback(final int partition, final String partitionString, final PutCommand request,
                final long messageId, final PutCallback cb) {
            this.partition = partition;
            this.partitionString = partitionString;
            this.request = request;
            this.messageId = messageId;
            this.cb = cb;
        }


        /**
         * Master append应答
         */
        @Override
        public void appendComplete(final Location location) {
            this.appendOffset = location.getOffset();
            if (location.isValid()) {
                synchronized (this) {
                    this.masterSuccess = true;
                }
            }
            this.tryComplete();
        }


        private synchronized void tryComplete() {
            if (this.respCount >= 2) {
                return;
            }
            this.respCount++;
            // 两者都应答了，可以给producer回馈
            if (this.respCount == 2) {
                // 仅在两者都成功的情况下，认为发送成功
                if (this.masterSuccess && this.slaveSuccess) {
                    final String resultStr =
                            SamsaCommandProcessor.this.genPutResultString(this.partition, this.messageId,
                                this.appendOffset);
                    if (this.cb != null) {
                        this.cb
                        .putComplete(new BooleanCommand(HttpStatus.Success, resultStr, this.request.getOpaque()));
                    }
                }
                else if (this.masterSuccess) {
                    SamsaCommandProcessor.this.statsManager.statsPutFailed(this.request.getTopic(),
                        this.partitionString, 1);
                    String error =
                            String.format("Put message to [slave '%s'] [partition '%s'] failed",
                                SamsaCommandProcessor.this.slaveUrl, this.request.getTopic() + "-" + this.partition);
                    this.cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, error, this.request
                        .getOpaque()));
                }
                else if (this.slaveSuccess) {
                    SamsaCommandProcessor.this.statsManager.statsPutFailed(this.request.getTopic(),
                        this.partitionString, 1);
                    String error =
                            String.format("Put message to [master '%s'] [partition '%s'] failed",
                                SamsaCommandProcessor.this.brokerZooKeeper.getBrokerString(), this.request.getTopic()
                                + "-" + this.partition);
                    this.cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, error, this.request
                        .getOpaque()));
                }
            }
        }


        @Override
        public void onResponse(final ResponseCommand responseCommand, final Connection conn) {
            // Slave响应成功
            if (responseCommand.getResponseStatus() == ResponseStatus.NO_ERROR) {
                SamsaCommandProcessor.this.slaveContinuousFailures.set(0);
                synchronized (this) {
                    this.slaveSuccess = true;
                }
            }
            else {
                if (SamsaCommandProcessor.this.slaveContinuousFailures.incrementAndGet() >= SamsaCommandProcessor.this.slaveContinuousFailureThreshold) {
                    SamsaCommandProcessor.this.slaveContinuousFailures.set(0);
                    SamsaCommandProcessor.this.removeMasterTemporaryAndTryToHeal();
                }
            }
            this.tryComplete();
        }


        @Override
        public void onException(final Exception e) {
            log.error("Put message to slave failed", e);
            this.slaveSuccess = false;
            this.tryComplete();
        }


        @Override
        public ThreadPoolExecutor getExecutor() {
            return SamsaCommandProcessor.this.callBackExecutor;
        }


        private SamsaCommandProcessor getOuterType() {
            return SamsaCommandProcessor.this;
        }
    }

    static final Log log = LogFactory.getLog(SamsaCommandProcessor.class);

    private RemotingClient remotingClient;

    private String slaveUrl;

    // 用于应答回调的线程池,caller run策略
    private ThreadPoolExecutor callBackExecutor;


    public SamsaCommandProcessor() {
        super();
    }


    public RemotingClient getRemotingClient() {
        return this.remotingClient;
    }


    void setRemotingClient(final RemotingClient remotingClient) {
        this.remotingClient = remotingClient;
    }


    public String getSlaveUrl() {
        return this.slaveUrl;
    }


    void setSlaveUrl(final String slaveUrl) {
        this.slaveUrl = slaveUrl;
    }


    public SamsaCommandProcessor(final MessageStoreManager storeManager, final ExecutorsManager executorsManager,
            final StatsManager statsManager, final RemotingServer remotingServer, final MetaConfig metaConfig,
            final IdWorker idWorker, final BrokerZooKeeper brokerZooKeeper, final RemotingClient remotingClient,
            final ConsumerFilterManager consumerFilterManager, final String slave, final int callbackThreadCount,
            long sendToSlaveTimeoutInMills, long checkSlaveIntervalInMills, int slaveContinuousFailureThreshold)
                    throws NotifyRemotingException, InterruptedException {
        super(storeManager, executorsManager, statsManager, remotingServer, metaConfig, idWorker, brokerZooKeeper,
            consumerFilterManager);
        this.slaveUrl = MetamorphosisWireFormatType.SCHEME + "://" + slave;
        this.remotingClient = remotingClient;
        this.sendToSlaveTimeoutInMills = sendToSlaveTimeoutInMills;
        this.checkSlaveIntervalInMills = checkSlaveIntervalInMills;
        this.slaveContinuousFailureThreshold = slaveContinuousFailureThreshold;
        this.callBackExecutor =
                new ThreadPoolExecutor(callbackThreadCount, callbackThreadCount, 60, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(10000), new ThreadPoolExecutor.CallerRunsPolicy());
        log.info("Connecting to slave broker:" + this.slaveUrl);
        this.remotingClient.connect(this.slaveUrl);
        try {
            this.remotingClient.awaitReadyInterrupt(this.slaveUrl);
        }
        catch (final NotifyRemotingException e) {
            log.error("Connect to salve broker[" + this.slaveUrl + "] failed,it will retry to connect it.", e);
        }
    }


    /**
     * 处理put请求，只有当master/slave全部写入成功的时候才认为写入成功
     */
    @Override
    public void processPutCommand(final PutCommand request, final SessionContext sessionContext, final PutCallback cb) {
        final String partitionString = this.metaConfig.getBrokerId() + "-" + request.getPartition();
        this.statsManager.statsPut(request.getTopic(), partitionString, 1);
        this.statsManager.statsMessageSize(request.getTopic(), request.getData().length);
        try {
            if (this.metaConfig.isClosedPartition(request.getTopic(), request.getPartition())) {
                log.warn("Can not put message to partition " + request.getPartition() + " for topic="
                        + request.getTopic() + ",it was closed");
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(HttpStatus.Forbidden, "Partition[" + partitionString
                        + "] has been closed", request.getOpaque()));
                }
                return;
            }
            // 如果是动态添加的topic，需要注册到zk
            this.brokerZooKeeper.registerTopicInZk(request.getTopic(), false);

            // 如果slave没有链接，马上返回失败，防止master重复消息过多
            if (!this.remotingClient.isConnected(this.slaveUrl)) {
                this.statsManager.statsPutFailed(request.getTopic(), partitionString, 1);
                cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, "Slave is disconnected ", request
                    .getOpaque()));
                return;
            }

            final int partition = this.getPartition(request);
            final MessageStore store = this.storeManager.getOrCreateMessageStore(request.getTopic(), partition);

            // 必须对store做同步，保证同一个分区内的消息有序
            synchronized (store) {
                // id也必须有序
                final long messageId = this.idWorker.nextId();
                // 构建callback
                final SyncAppendCallback syncCB =
                        new SyncAppendCallback(partition, partitionString, request, messageId, cb);
                // 发往slave
                this.remotingClient.sendToGroup(this.slaveUrl,
                    new SyncCommand(request.getTopic(), partition, request.getData(), request.getFlag(), messageId,
                        request.getCheckSum(), OpaqueGenerator.getNextOpaque()), syncCB,
                        this.sendToSlaveTimeoutInMills, TimeUnit.MILLISECONDS);
                // 写入master
                store.append(messageId, request, syncCB);
            }
        }
        catch (final Exception e) {
            this.statsManager.statsPutFailed(request.getTopic(), partitionString, 1);
            log.error("Put message failed", e);
            if (cb != null) {
                cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, e.getMessage(), request.getOpaque()));
            }
        }
    }
}