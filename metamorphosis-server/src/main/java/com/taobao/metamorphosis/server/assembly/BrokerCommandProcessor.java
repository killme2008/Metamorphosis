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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.network.DataCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.MetaEncodeCommand;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.exception.MetamorphosisException;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.AppendCallback;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageSet;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.server.utils.BuildProperties;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.IdWorker;


/**
 * meta服务端核心处理器
 * 
 * @author boyan
 * 
 */
public class BrokerCommandProcessor implements CommandProcessor {
    /**
     * append到message store的callback
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-12-7
     * 
     */
    public final class StoreAppendCallback implements AppendCallback {
        private final int partition;
        private final String partitionString;
        private final PutCommand request;
        private final long messageId;
        private final PutCallback cb;


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.getOuterType().hashCode();
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
            final StoreAppendCallback other = (StoreAppendCallback) obj;
            if (!this.getOuterType().equals(other.getOuterType())) {
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


        public StoreAppendCallback(final int partition, final String partitionString, final PutCommand request,
                final long messageId, final PutCallback cb) {
            this.partition = partition;
            this.partitionString = partitionString;
            this.request = request;
            this.messageId = messageId;
            this.cb = cb;
        }


        @Override
        public void appendComplete(final Location location) {
            if (location.isValid()) {
                final String resultStr =
                        BrokerCommandProcessor.this.genPutResultString(this.partition, this.messageId,
                            location.getOffset());
                if (this.cb != null) {
                    this.cb.putComplete(new BooleanCommand(HttpStatus.Success, resultStr, this.request.getOpaque()));
                }
            }
            else {
                BrokerCommandProcessor.this.statsManager.statsPutFailed(this.request.getTopic(), this.partitionString,
                    1);
                if (this.cb != null) {
                    String error = BrokerCommandProcessor.this.genErrorMessage(this.request.getTopic(), this.partition);
                    this.cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, error, this.request
                        .getOpaque()));
                }
            }

        }


        private BrokerCommandProcessor getOuterType() {
            return BrokerCommandProcessor.this;
        }
    }

    static final Log log = LogFactory.getLog(BrokerCommandProcessor.class);

    protected MessageStoreManager storeManager;
    protected ExecutorsManager executorsManager;
    protected StatsManager statsManager;
    protected RemotingServer remotingServer;
    protected MetaConfig metaConfig;
    protected IdWorker idWorker;
    protected BrokerZooKeeper brokerZooKeeper;
    protected ConsumerFilterManager consumerFilterManager;


    protected String genErrorMessage(String topic, int partition) {
        String error =
                String.format("Put message to [broker '%s'] [partition '%s'] failed.",
                    this.brokerZooKeeper.getBrokerString(), topic + "-" + partition);
        return error;
    }


    /**
     * 仅用于测试
     */
    public BrokerCommandProcessor() {
        super();
    }


    public BrokerCommandProcessor(final MessageStoreManager storeManager, final ExecutorsManager executorsManager,
            final StatsManager statsManager, final RemotingServer remotingServer, final MetaConfig metaConfig,
            final IdWorker idWorker, final BrokerZooKeeper brokerZooKeeper,
            final ConsumerFilterManager consumerFilterManager) {
        super();
        this.storeManager = storeManager;
        this.executorsManager = executorsManager;
        this.statsManager = statsManager;
        this.remotingServer = remotingServer;
        this.metaConfig = metaConfig;
        this.idWorker = idWorker;
        this.brokerZooKeeper = brokerZooKeeper;
        this.consumerFilterManager = consumerFilterManager;
    }


    public ConsumerFilterManager getConsumerFilterManager() {
        return this.consumerFilterManager;
    }


    public void setConsumerFilterManager(ConsumerFilterManager consumerFilterManager) {
        this.consumerFilterManager = consumerFilterManager;
    }


    public MessageStoreManager getStoreManager() {
        return this.storeManager;
    }


    public void setStoreManager(final MessageStoreManager storeManager) {
        this.storeManager = storeManager;
    }


    public ExecutorsManager getExecutorsManager() {
        return this.executorsManager;
    }


    public void setExecutorsManager(final ExecutorsManager executorsManager) {
        this.executorsManager = executorsManager;
    }


    public StatsManager getStatsManager() {
        return this.statsManager;
    }


    public void setStatsManager(final StatsManager statsManager) {
        this.statsManager = statsManager;
    }


    public RemotingServer getRemotingServer() {
        return this.remotingServer;
    }


    public void setRemotingServer(final RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public MetaConfig getMetaConfig() {
        return this.metaConfig;
    }


    public void setMetaConfig(final MetaConfig metaConfig) {
        this.metaConfig = metaConfig;
    }


    public IdWorker getIdWorker() {
        return this.idWorker;
    }


    public void setIdWorker(final IdWorker idWorker) {
        this.idWorker = idWorker;
    }


    public BrokerZooKeeper getBrokerZooKeeper() {
        return this.brokerZooKeeper;
    }


    public void setBrokerZooKeeper(final BrokerZooKeeper brokerZooKeeper) {
        this.brokerZooKeeper = brokerZooKeeper;
    }


    @Override
    public void init() {

    }


    @Override
    public void dispose() {

    }


    @Override
    public void processPutCommand(final PutCommand request, final SessionContext sessionContext, final PutCallback cb) {
        final String partitionString = this.metaConfig.getBrokerId() + "-" + request.getPartition();
        this.statsManager.statsPut(request.getTopic(), partitionString, 1);
        this.statsManager.statsMessageSize(request.getTopic(), request.getData().length);
        int partition = -1;
        try {
            if (this.metaConfig.isClosedPartition(request.getTopic(), request.getPartition())) {
                log.warn("Can not put message to partition " + request.getPartition() + " for topic="
                        + request.getTopic() + ",it was closed");
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(HttpStatus.Forbidden, this.genErrorMessage(request.getTopic(),
                        request.getPartition()) + "Detail:partition[" + partitionString + "] has been closed", request
                        .getOpaque()));
                }
                return;
            }

            partition = this.getPartition(request);
            final MessageStore store = this.storeManager.getOrCreateMessageStore(request.getTopic(), partition);
            // 如果是动态添加的topic，需要注册到zk
            this.brokerZooKeeper.registerTopicInZk(request.getTopic(), false);
            // 设置唯一id
            final long messageId = this.idWorker.nextId();
            store.append(messageId, request,
                new StoreAppendCallback(partition, partitionString, request, messageId, cb));
        }
        catch (final Exception e) {
            this.statsManager.statsPutFailed(request.getTopic(), partitionString, 1);
            log.error("Put message failed", e);
            if (cb != null) {
                cb.putComplete(new BooleanCommand(HttpStatus.InternalServerError, this.genErrorMessage(
                    request.getTopic(), partition)
                    + "Detail:" + e.getMessage(), request.getOpaque()));
            }
        }
    }


    protected int getPartition(final PutCommand request) {
        int partition = request.getPartition();
        if (partition == Partition.RandomPartiton.getPartition()) {
            partition = this.storeManager.chooseRandomPartition(request.getTopic());
        }
        return partition;
    }


    /**
     * 返回形如"messageId partition offset"的字符号，返回给客户端
     * 
     * @param partition
     * @param messageId
     * @param offset
     * @return
     */
    protected String genPutResultString(final int partition, final long messageId, final long offset) {
        final StringBuilder sb =
                new StringBuilder(ByteUtils.stringSize(offset) + ByteUtils.stringSize(messageId)
                    + ByteUtils.stringSize(partition) + 2);
        sb.append(messageId).append(" ").append(partition).append(" ").append(offset);
        return sb.toString();
    }


    @Override
    public ResponseCommand processGetCommand(final GetCommand request, final SessionContext ctx) {
        return this.processGetCommand(request, ctx, true);
    }


    @Override
    public ResponseCommand processGetCommand(final GetCommand request, final SessionContext ctx, final boolean zeroCopy) {
        final String group = request.getGroup();
        final String topic = request.getTopic();
        this.statsManager.statsGet(topic, group, 1);

        // 如果分区被关闭,禁止读数据 --wuhua
        if (this.metaConfig.isClosedPartition(topic, request.getPartition())) {
            log.warn("can not get message for topic=" + topic + " from partition " + request.getPartition()
                + ",it closed,");
            return new BooleanCommand(HttpStatus.Forbidden, "Partition[" + this.metaConfig.getBrokerId() + "-"
                    + request.getPartition() + "] has been closed", request.getOpaque());
        }

        final MessageStore store = this.storeManager.getMessageStore(topic, request.getPartition());
        if (store == null) {
            this.statsManager.statsGetMiss(topic, group, 1);
            return new BooleanCommand(HttpStatus.NotFound, "The topic `" + topic + "` in partition `"
                    + request.getPartition() + "` is not exists", request.getOpaque());
        }
        if (request.getMaxSize() <= 0) {
            return new BooleanCommand(HttpStatus.BadRequest, "Bad request,invalid max size:" + request.getMaxSize(),
                request.getOpaque());
        }
        try {
            final MessageSet set =
                    store.slice(request.getOffset(),
                        Math.min(this.metaConfig.getMaxTransferSize(), request.getMaxSize()));
            ConsumerMessageFilter filter = this.consumerFilterManager.findFilter(topic, group);
            if (set != null) {
                if (zeroCopy && filter == null) {
                    set.write(request, ctx);
                    return null;
                }
                else {
                    // refer to the code of line 440 in MessageStore
                    // create two copies of byte array including the byteBuffer
                    // and new bytes
                    // this may not a good use case of Buffer
                    final ByteBuffer byteBuffer =
                            ByteBuffer.allocate(Math.min(this.metaConfig.getMaxTransferSize(), request.getMaxSize()));
                    set.read(byteBuffer);
                    byte[] bytes = this.getBytesFromBuffer(byteBuffer);
                    // If filter is not null,we filter the messages by it.
                    if (filter != null) {
                        MessageIterator it = new MessageIterator(topic, bytes);
                        // reuse the buffer.
                        byteBuffer.clear();
                        while (it.hasNext()) {
                            Message msg = it.next();
                            try {
                                if (filter.accept(group, msg)) {
                                    ByteBuffer msgBuf = it.getCurrentMsgBuf();
                                    // Append current message buffer to result
                                    // buffer.
                                    byteBuffer.put(msgBuf);
                                }
                            }
                            catch (Exception e) {
                                log.error("Filter message for consumer failed,topic=" + topic + ",group=" + group
                                    + ",filterClass=" + filter.getClass().getCanonicalName(), e);
                            }
                        }
                        // re-new the byte array.
                        bytes = this.getBytesFromBuffer(byteBuffer);
                        // All these messages are not acceptable,move forward
                        // offset.
                        if (bytes.length == 0) {
                            return new BooleanCommand(HttpStatus.Moved, String.valueOf(request.getOffset()
                                + it.getOffset()), request.getOpaque());
                        }
                    }
                    return new DataCommand(bytes, request.getOpaque(), true);
                }
            }
            else {
                this.statsManager.statsGetMiss(topic, group, 1);
                this.statsManager.statsGetFailed(topic, group, 1);

                // 当请求的偏移量大于实际最大值时,返回给客户端实际最大的偏移量.
                final long maxOffset = store.getMaxOffset();
                final long requestOffset = request.getOffset();
                if (requestOffset > maxOffset
                        && (this.metaConfig.isUpdateConsumerOffsets() || requestOffset == Long.MAX_VALUE)) {
                    log.info("offset[" + requestOffset + "] is exceeded,tell the client real max offset: " + maxOffset
                        + ",topic=" + topic + ",group=" + group);
                    this.statsManager.statsOffset(topic, group, 1);
                    return new BooleanCommand(HttpStatus.Moved, String.valueOf(maxOffset), request.getOpaque());
                }
                else {
                    return new BooleanCommand(HttpStatus.NotFound, "Could not find message at position "
                            + requestOffset, request.getOpaque());
                }
            }
        }
        catch (final ArrayIndexOutOfBoundsException e) {
            log.error("Could not get message from position " + request.getOffset() + ",it is out of bounds,topic="
                    + topic);
            // 告知最近可用的offset
            this.statsManager.statsGetMiss(topic, group, 1);
            this.statsManager.statsGetFailed(topic, group, 1);
            final long validOffset = store.getNearestOffset(request.getOffset());
            this.statsManager.statsOffset(topic, group, 1);
            return new BooleanCommand(HttpStatus.Moved, String.valueOf(validOffset), request.getOpaque());

        }
        catch (final Throwable e) {
            log.error("Could not get message from position " + request.getOffset(), e);
            this.statsManager.statsGetFailed(topic, group, 1);
            return new BooleanCommand(HttpStatus.InternalServerError, this.genErrorMessage(request.getTopic(),
                request.getPartition())
                + "Detail:" + e.getMessage(), request.getOpaque());
        }

    }


    private byte[] getBytesFromBuffer(final ByteBuffer byteBuffer) {
        byteBuffer.flip();
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }


    @Override
    public ResponseCommand processOffsetCommand(final OffsetCommand request, final SessionContext ctx) {
        this.statsManager.statsOffset(request.getTopic(), request.getGroup(), 1);
        final MessageStore store = this.storeManager.getMessageStore(request.getTopic(), request.getPartition());
        if (store == null) {
            return new BooleanCommand(HttpStatus.NotFound, "The topic `" + request.getTopic() + "` in partition `"
                    + request.getPartition() + "` is not exists", request.getOpaque());
        }
        final long offset = store.getNearestOffset(request.getOffset());
        return new BooleanCommand(HttpStatus.Success, String.valueOf(offset), request.getOpaque());

    }


    @Override
    public void processQuitCommand(final QuitCommand request, final SessionContext ctx) {
        try {
            if (ctx.getConnection() != null) {
                ctx.getConnection().close(false);
            }
        }
        catch (final NotifyRemotingException e) {
            // ignore
        }

    }


    @Override
    public ResponseCommand processVesionCommand(final VersionCommand request, final SessionContext ctx) {
        return new BooleanCommand(HttpStatus.Success, BuildProperties.VERSION, request.getOpaque());

    }


    @Override
    public ResponseCommand processStatCommand(final StatsCommand request, final SessionContext ctx) {
        final String item = request.getItem();
        if ("config".equals(item)) {
            return this.processStatsConfig(request, ctx);
        }
        else {
            final String statsInfo = this.statsManager.getStatsInfo(item);
            return new BooleanCommand(HttpStatus.Success, statsInfo, request.getOpaque());
        }
    }


    private ResponseCommand processStatsConfig(final StatsCommand request, final SessionContext ctx) {
        try {
            final FileChannel fc = new FileInputStream(this.metaConfig.getConfigFilePath()).getChannel();
            // result code length opaque\r\n
            IoBuffer buf =
                    IoBuffer.allocate(11 + 3 + ByteUtils.stringSize(fc.size())
                        + ByteUtils.stringSize(request.getOpaque()));
            ByteUtils.setArguments(buf, MetaEncodeCommand.RESULT_CMD, HttpStatus.Success, fc.size(),
                request.getOpaque());
            buf.flip();
            ctx.getConnection().transferFrom(buf, null, fc, 0, fc.size(), request.getOpaque(),
                new SingleRequestCallBackListener() {

                @Override
                public void onResponse(ResponseCommand responseCommand, Connection conn) {
                    this.closeChannel();
                }


                @Override
                public void onException(Exception e) {
                    this.closeChannel();
                }


                private void closeChannel() {
                    try {
                        fc.close();
                    }
                    catch (IOException e) {
                        log.error("IOException while stats config", e);
                    }
                }


                @Override
                public ThreadPoolExecutor getExecutor() {
                    return null;
                }
            }, 5000, TimeUnit.MILLISECONDS);
        }
        catch (FileNotFoundException e) {
            log.error("Config file not found:" + this.metaConfig.getConfigFilePath(), e);
            return new BooleanCommand(HttpStatus.InternalServerError, "Config file not found:"
                    + this.metaConfig.getConfigFilePath(), request.getOpaque());
        }
        catch (IOException e) {
            log.error("IOException while stats config", e);
            return new BooleanCommand(HttpStatus.InternalServerError, "Read config file error:" + e.getMessage(),
                request.getOpaque());
        }
        catch (NotifyRemotingException e) {
            log.error("NotifyRemotingException while stats config", e);
        }
        return null;
    }


    @Override
    public void removeTransaction(final XATransactionId xid) {
        throw new UnsupportedOperationException("Unsupported removeTransaction");
    }


    @Override
    public Transaction getTransaction(final SessionContext context, final TransactionId xid)
            throws MetamorphosisException, XAException {
        throw new UnsupportedOperationException("Unsupported getTransaction");
    }


    @Override
    public void forgetTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        throw new UnsupportedOperationException("Unsupported forgetTransaction");
    }


    @Override
    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        throw new UnsupportedOperationException("Unsupported rollbackTransaction");
    }


    @Override
    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase)
            throws Exception {
        throw new UnsupportedOperationException("Unsupported commitTransaction");
    }


    @Override
    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        throw new UnsupportedOperationException("Unsupported prepareTransaction");
    }


    @Override
    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds)
            throws Exception {
        throw new UnsupportedOperationException("Unsupported beginTransaction");
    }


    @Override
    public TransactionId[] getPreparedTransactions(final SessionContext context, String uniqueQualifier)
            throws Exception {
        throw new UnsupportedOperationException("Unsupported getPreparedTransactions");
    }

}