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
package com.taobao.metamorphosis.client.producer;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import javax.transaction.xa.XAException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.util.ConcurrentHashSet;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.transaction.TransactionContext;
import com.taobao.metamorphosis.client.transaction.TransactionSession;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.exception.MetaOpeartionTimeoutException;
import com.taobao.metamorphosis.exception.TransactionInProgressException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.LongSequenceGenerator;
import com.taobao.metamorphosis.utils.MessageFlagUtils;
import com.taobao.metamorphosis.utils.MessageUtils;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * 消费生产者实现
 * 
 * @author boyan
 * @Date 2011-4-21
 * @author wuhua
 * @Date 2011-8-3
 */
public class SimpleMessageProducer implements MessageProducer, TransactionSession {
    private static final Log log = LogFactory.getLog(SimpleMessageProducer.class);
    protected static final long DEFAULT_OP_TIMEOUT = 3000L;
    private static final int TIMEOUT_THRESHOLD = Integer.parseInt(System.getProperty("meta.send.timeout.threshold",
            "200"));
    private final MetaMessageSessionFactory messageSessionFactory;
    protected final RemotingClientWrapper remotingClient;
    protected final PartitionSelector partitionSelector;
    protected final ProducerZooKeeper producerZooKeeper;
    protected final String sessionId;
    // 默认事务超时时间为10秒
    protected volatile int transactionTimeout = 0;
    // private final boolean ordered;
    private volatile boolean shutdown;
    private static final int MAX_RETRY = 1;
    private final LongSequenceGenerator localTxIdGenerator;
    private final ConcurrentHashSet<String> publishedTopics = new ConcurrentHashSet<String>();
    protected long transactionRequestTimeoutInMills = 5000;


    public SimpleMessageProducer(final MetaMessageSessionFactory messageSessionFactory,
            final RemotingClientWrapper remotingClient, final PartitionSelector partitionSelector,
            final ProducerZooKeeper producerZooKeeper, final String sessionId) {
        super();
        this.sessionId = sessionId;
        this.messageSessionFactory = messageSessionFactory;
        this.remotingClient = remotingClient;
        this.partitionSelector = partitionSelector;
        this.producerZooKeeper = producerZooKeeper;
        this.localTxIdGenerator = new LongSequenceGenerator();

        // this.ordered = ordered;
    }


    @Override
    public void setTransactionRequestTimeout(long time, TimeUnit timeUnit) {
        if (timeUnit == null) {
            throw new IllegalArgumentException("Invalid time unit");
        }
        this.transactionRequestTimeoutInMills = TimeUnit.MILLISECONDS.convert(time, timeUnit);
    }


    long getTransactionRequestTimeoutInMills() {
        return this.transactionRequestTimeoutInMills;
    }

    public MetaMessageSessionFactory getParent() {
        return this.messageSessionFactory;
    }


    @Override
    public PartitionSelector getPartitionSelector() {
        return this.partitionSelector;
    }


    @Override
    @Deprecated
    public boolean isOrdered() {
        return false;
    }


    @Override
    public void publish(final String topic) {
        this.checkState();
        this.checkTopic(topic);
        // It is not always synchronized with shutdown,but it is acceptable.
        if (!this.publishedTopics.contains(topic)) {
            this.producerZooKeeper.publishTopic(topic, this);
            this.publishedTopics.add(topic);
        }
        // this.localMessageStorageManager.setMessageRecoverer(this.recoverer);
    }


    @Override
    public void setDefaultTopic(final String topic) {
        // It is not always synchronized with shutdown,but it is acceptable.
        if (!this.publishedTopics.contains(topic)) {
            this.producerZooKeeper.setDefaultTopic(topic, this);
            this.publishedTopics.add(topic);
        }
    }


    private void checkTopic(final String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic:" + topic);
        }
    }

    static final Pattern RESULT_SPLITER = Pattern.compile(" ");


    @Override
    public SendResult sendMessage(final Message message, final long timeout, final TimeUnit unit)
            throws MetaClientException, InterruptedException {
        this.checkState();
        this.checkMessage(message);
        return this.sendMessageToServer(message, timeout, unit);
    }


    /** 正常的消息发送到服务器 */
    protected SendResult sendMessageToServer(final Message message, final long timeout, final TimeUnit unit)
            throws MetaClientException, InterruptedException, MetaOpeartionTimeoutException {
        /**
         * 异常信息处理的原则：
         * <ul>
         * <li>客户端错误信息都以异常的形式抛出，全部转化为MetaClientException的check异常</li>
         * <li>服务器返回失败的异常信息，作为SendResult的结果返回给客户端，不作为异常抛出</li>
         * </ul>
         */
        // this.checkState();
        // this.checkMessage(message);
        SendResult result = null;
        final long start = System.currentTimeMillis();
        int retry = 0;
        final long timeoutInMills = TimeUnit.MILLISECONDS.convert(timeout, unit);
        final byte[] data = MessageUtils.encodePayload(message);
        try {
            for (int i = 0; i < MAX_RETRY; i++) {
                result = this.send0(message, data, timeout, unit);
                if (result.isSuccess()) {
                    break;
                }
                if (System.currentTimeMillis() - start >= timeoutInMills) {
                    throw new MetaOpeartionTimeoutException("Send message timeout in " + timeoutInMills + " mills");
                }
                retry++;
            }
        }
        finally {
            final long duration = System.currentTimeMillis() - start;
            MetaStatLog.addStatValue2(null, StatConstants.PUT_TIME_STAT, message.getTopic(), duration);
            if (duration > TIMEOUT_THRESHOLD) {
                MetaStatLog.addStatValue2(null, StatConstants.PUT_TIMEOUT_STAT, message.getTopic(), duration);
            }
            if (retry > 0) {
                MetaStatLog.addStatValue2(null, StatConstants.PUT_RETRY_STAT, message.getTopic(), retry);
            }
        }
        return result;
    }


    private Partition selectPartition(final Message message) throws MetaClientException {
        return this.producerZooKeeper.selectPartition(message.getTopic(), message, this.partitionSelector);
    }

    /**
     * 事务相关代码
     * 
     * @author boyan
     * @date 2011-08-17
     */
    protected final ThreadLocal<LastSentInfo> lastSentInfo = new ThreadLocal<LastSentInfo>();

    /**
     * 与线程关联的事务上下文
     */
    final protected ThreadLocal<TransactionContext> transactionContext = new ThreadLocal<TransactionContext>();

    // 记录事务内上一次发送消息的信息，主要是为了记录第一次发送消息的serverUrl
    static class LastSentInfo {
        final String serverUrl;


        public LastSentInfo(final String serverUrl) {
            super();
            this.serverUrl = serverUrl;
        }

    }


    private TransactionContext getTx() throws MetaClientException {
        final TransactionContext ctx = this.transactionContext.get();
        if (ctx == null) {
            throw new MetaClientException("There is no transaction begun");
        }
        return ctx;
    }


    @Override
    public void removeContext(final TransactionContext ctx) {
        assert this.transactionContext.get() == ctx;
        this.transactionContext.remove();
        this.resetLastSentInfo();
    }


    @Override
    public String getSessionId() {
        return this.sessionId;
    }


    @Override
    public void setTransactionTimeout(final int seconds) throws MetaClientException {
        if (seconds < 0) {
            throw new IllegalArgumentException("Illegal transaction timeout value");
        }
        this.transactionTimeout = seconds;

    }


    @Override
    public int getTransactionTimeout() throws MetaClientException {
        return this.transactionTimeout;
    }


    /**
     * 开启一个事务并关联到当前线程
     * 
     * @throws MetaClientException
     *             如果已经处于事务中，则抛出TransactionInProgressException异常
     */
    @Override
    public void beginTransaction() throws MetaClientException {
        // 没有在此方法里调用begin，而是等到第一次发送消息前才调用
        TransactionContext ctx = this.transactionContext.get();
        if (ctx == null) {
            ctx =
                    new TransactionContext(this.remotingClient, null, this, this.localTxIdGenerator,
                        this.transactionTimeout, this.transactionRequestTimeoutInMills);
            this.transactionContext.set(ctx);
        }
        else {
            throw new TransactionInProgressException("A transaction has begun");
        }

    }


    /**
     * 在第一次发送前开始事务
     * 
     * @param serverUrl
     * @throws MetaClientException
     */
    protected void beforeSendMessageFirstTime(final String serverUrl) throws MetaClientException, XAException {
        final TransactionContext tx = this.getTx();
        // 本地事务，则需要设置serverUrl并begin
        if (tx.getTransactionId() == null) {
            tx.setServerUrl(serverUrl);
            tx.begin();
        }
    }


    /**
     * 记录上一次投递信息
     * 
     * @param serverUrl
     */
    protected void logLastSentInfo(final String serverUrl) {
        if (this.isInTransaction() && this.lastSentInfo.get() == null) {
            this.lastSentInfo.set(new LastSentInfo(serverUrl));
        }
    }


    /**
     * 返回事务id
     * 
     * @return
     * @throws MetaClientException
     */
    protected TransactionId getTransactionId() throws MetaClientException {
        if (this.isInTransaction()) {
            return this.getTx().getTransactionId();
        }
        else {
            return null;
        }
    }


    /**
     * 判断是否处于事务中
     * 
     * @return
     */
    protected boolean isInTransaction() {
        return this.transactionContext.get() != null;
    }


    /**
     * 提交事务，将事务内发送的消息持久化，此方法仅能在beginTransaction之后调用
     * 
     * @see #beginTransaction()
     * @throws MetaClientException
     */
    @Override
    public void commit() throws MetaClientException {
        try {
            final TransactionContext ctx = this.getTx();
            ctx.commit();
        }
        finally {
            this.resetLastSentInfo();
            this.transactionContext.remove();
        }
    }


    /**
     * 回滚事务内所发送的任何消息，此方法仅能在beginTransaction之后调用
     * 
     * @throws MetaClientException
     * @see #beginTransaction()
     */
    @Override
    public void rollback() throws MetaClientException {
        try {
            final TransactionContext ctx = this.getTx();
            ctx.rollback();
        }
        finally {
            this.resetLastSentInfo();
            this.transactionContext.remove();
        }
    }


    protected void resetLastSentInfo() {
        this.lastSentInfo.remove();
    }


    private SendResult send0(final Message message, final byte[] encodedData, final long timeout, final TimeUnit unit)
            throws InterruptedException, MetaClientException {
        try {
            final String topic = message.getTopic();
            Partition partition = null;
            String serverUrl = null;
            // 如果在事务内，则使用上一次发送消息时选择的broker
            if (this.isInTransaction()) {
                final LastSentInfo info = this.lastSentInfo.get();
                if (info != null) {
                    serverUrl = info.serverUrl;
                    // 选择该broker内的某个分区
                    partition =
                            this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector, serverUrl);
                    if (partition == null) {
                        // 没有可用分区，抛出异常
                        throw new MetaClientException("There is no partitions in `" + serverUrl
                            + "` to send message with topic `" + topic + "` in a transaction");
                    }
                }
            }
            if (partition == null) {
                partition = this.selectPartition(message);
            }
            if (partition == null) {
                throw new MetaClientException("There is no aviable partition for topic " + topic
                    + ",maybe you don't publish it at first?");
            }
            if (serverUrl == null) {
                serverUrl = this.producerZooKeeper.selectBroker(topic, partition);
            }
            if (serverUrl == null) {
                throw new MetaClientException("There is no aviable server right now for topic " + topic
                    + " and partition " + partition + ",maybe you don't publish it at first?");
            }

            if (this.isInTransaction() && this.lastSentInfo.get() == null) {
                // 第一次发送，需要启动事务
                this.beforeSendMessageFirstTime(serverUrl);
            }

            final int flag = MessageFlagUtils.getFlag(message);
            final PutCommand putCommand =
                    new PutCommand(topic, partition.getPartition(), encodedData, flag, CheckSum.crc32(encodedData),
                        this.getTransactionId(), OpaqueGenerator.getNextOpaque());
            final BooleanCommand resp = this.invokeToGroup(serverUrl, partition, putCommand, message, timeout, unit);
            return this.genSendResult(message, partition, serverUrl, resp);
        }
        catch (final TimeoutException e) {
            throw new MetaOpeartionTimeoutException("Send message timeout in "
                    + TimeUnit.MILLISECONDS.convert(timeout, unit) + " mills");
        }
        catch (final InterruptedException e) {
            throw e;
        }
        catch (final MetaClientException e) {
            throw e;
        }
        catch (final Exception e) {
            throw new MetaClientException("send message failed", e);
        }
    }


    private SendResult genSendResult(final Message message, final Partition partition, final String serverUrl,
            final BooleanCommand resp) {
        final String resultStr = resp.getErrorMsg();

        switch (resp.getCode()) {
        case HttpStatus.Success: {
            // messageId partition offset
            final String[] tmps = RESULT_SPLITER.split(resultStr);
            // 成功，设置消息id，消息id由服务器产生
            MessageAccessor.setId(message, Long.parseLong(tmps[0]));
            final Partition serverPart = new Partition(partition.getBrokerId(), Integer.parseInt(tmps[1]));
            MessageAccessor.setPartition(message, serverPart);
            // 记录本次发送信息，仅用于事务消息
            this.logLastSentInfo(serverUrl);
            return new SendResult(true, serverPart, Long.parseLong(tmps[2]), null);
        }
        case HttpStatus.Forbidden: {
            if (log.isDebugEnabled()) {
                log.debug(resultStr);
            }
            return new SendResult(false, null, -1, String.valueOf(HttpStatus.Forbidden));
        }
        default:
            return new SendResult(false, null, -1, resultStr);
        }
    }


    protected BooleanCommand invokeToGroup(final String serverUrl, final Partition partition,
            final PutCommand putCommand, final Message message, final long timeout, final TimeUnit unit)
                    throws InterruptedException, TimeoutException, NotifyRemotingException {

        return (BooleanCommand) this.remotingClient.invokeToGroup(serverUrl, putCommand, timeout, unit);
    }


    protected void checkState() {
        if (this.shutdown) {
            throw new IllegalStateException("Producer has been shutdown");
        }
    }


    protected void checkMessage(final Message message) throws MetaClientException {
        if (message == null) {
            throw new InvalidMessageException("Null message");
        }
        if (StringUtils.isBlank(message.getTopic())) {
            throw new InvalidMessageException("Blank topic");
        }
        if (message.getData() == null) {
            throw new InvalidMessageException("Null data");
        }
    }


    @Override
    public void sendMessage(final Message message, final SendMessageCallback cb, final long time, final TimeUnit unit) {
        try {
            final String topic = message.getTopic();
            final Partition partition = this.selectPartition(message);
            if (partition == null) {
                throw new MetaClientException("There is no aviable partition for topic " + topic
                    + ",maybe you don't publish it at first?");
            }
            final String serverUrl = this.producerZooKeeper.selectBroker(topic, partition);
            if (serverUrl == null) {
                throw new MetaClientException("There is no aviable server right now for topic " + topic
                    + " and partition " + partition + ",maybe you don't publish it at first?");
            }

            final int flag = MessageFlagUtils.getFlag(message);
            final byte[] encodedData = MessageUtils.encodePayload(message);
            final PutCommand putCommand =
                    new PutCommand(topic, partition.getPartition(), encodedData, flag, CheckSum.crc32(encodedData),
                        this.getTransactionId(), OpaqueGenerator.getNextOpaque());
            this.remotingClient.sendToGroup(serverUrl, putCommand, new SingleRequestCallBackListener() {
                @Override
                public void onResponse(final ResponseCommand responseCommand, final Connection conn) {
                    final SendResult rt =
                            SimpleMessageProducer.this.genSendResult(message, partition, serverUrl,
                                (BooleanCommand) responseCommand);
                    cb.onMessageSent(rt);
                }


                @Override
                public void onException(final Exception e) {
                    cb.onException(e);
                }


                @Override
                public ThreadPoolExecutor getExecutor() {
                    return null;
                }
            }, time, unit);

        }
        catch (final Throwable e) {
            cb.onException(e);
        }

    }


    @Override
    public void sendMessage(final Message message, final SendMessageCallback cb) {
        this.sendMessage(message, cb, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
    }


    @Override
    public SendResult sendMessage(final Message message) throws MetaClientException, InterruptedException {
        return this.sendMessage(message, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
    }


    @Override
    public synchronized void shutdown() throws MetaClientException {
        if (this.shutdown) {
            return;
        }
        for (String topic : this.publishedTopics) {
            this.producerZooKeeper.unPublishTopic(topic, this);
        }
        this.shutdown = true;
        this.publishedTopics.clear();
        this.messageSessionFactory.removeChild(this);
    }

}