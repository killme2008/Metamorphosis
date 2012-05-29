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

import java.nio.ByteBuffer;
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
import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.utils.LongSequenceGenerator;
import com.taobao.metamorphosis.utils.MessageFlagUtils;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * ����������ʵ��
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
    // Ĭ������ʱʱ��Ϊ10��
    protected volatile int transactionTimeout = 0;
    // private final boolean ordered;
    private volatile boolean shutdown;
    private static final int MAX_RETRY = 1;
    private final LongSequenceGenerator localTxIdGenerator;
    private final ConcurrentHashSet<String> publishedTopics = new ConcurrentHashSet<String>();


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


    /** ��������Ϣ���͵������� */
    protected SendResult sendMessageToServer(final Message message, final long timeout, final TimeUnit unit)
            throws MetaClientException, InterruptedException, MetaOpeartionTimeoutException {
        /**
         * �쳣��Ϣ�����ԭ��
         * <ul>
         * <li>�ͻ��˴�����Ϣ�����쳣����ʽ�׳���ȫ��ת��ΪMetaClientException��check�쳣</li>
         * <li>����������ʧ�ܵ��쳣��Ϣ����ΪSendResult�Ľ�����ظ��ͻ��ˣ�����Ϊ�쳣�׳�</li>
         * </ul>
         */
        // this.checkState();
        // this.checkMessage(message);
        SendResult result = null;
        final long start = System.currentTimeMillis();
        int retry = 0;
        final long timeoutInMills = TimeUnit.MILLISECONDS.convert(timeout, unit);
        final byte[] data = SimpleMessageProducer.encodeData(message);
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
     * ������ش���
     * 
     * @author boyan
     * @date 2011-08-17
     */
    protected final ThreadLocal<LastSentInfo> lastSentInfo = new ThreadLocal<LastSentInfo>();

    /**
     * ���̹߳���������������
     */
    final protected ThreadLocal<TransactionContext> transactionContext = new ThreadLocal<TransactionContext>();

    // ��¼��������һ�η�����Ϣ����Ϣ����Ҫ��Ϊ�˼�¼��һ�η�����Ϣ��serverUrl
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
     * ����һ�����񲢹�������ǰ�߳�
     * 
     * @throws MetaClientException
     *             ����Ѿ����������У����׳�TransactionInProgressException�쳣
     */
    @Override
    public void beginTransaction() throws MetaClientException {
        // û���ڴ˷��������begin�����ǵȵ���һ�η�����Ϣǰ�ŵ���
        TransactionContext ctx = this.transactionContext.get();
        if (ctx == null) {
            ctx =
                    new TransactionContext(this.remotingClient, null, this, this.localTxIdGenerator,
                        this.transactionTimeout);
            this.transactionContext.set(ctx);
        }
        else {
            throw new TransactionInProgressException("A transaction has begun");
        }

    }


    /**
     * �ڵ�һ�η���ǰ��ʼ����
     * 
     * @param serverUrl
     * @throws MetaClientException
     */
    protected void beforeSendMessageFirstTime(final String serverUrl) throws MetaClientException, XAException {
        final TransactionContext tx = this.getTx();
        // ������������Ҫ����serverUrl��begin
        if (tx.getTransactionId() == null) {
            tx.setServerUrl(serverUrl);
            tx.begin();
        }
    }


    /**
     * ��¼��һ��Ͷ����Ϣ
     * 
     * @param serverUrl
     */
    protected void logLastSentInfo(final String serverUrl) {
        if (this.isInTransaction() && this.lastSentInfo.get() == null) {
            this.lastSentInfo.set(new LastSentInfo(serverUrl));
        }
    }


    /**
     * ��������id
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
     * �ж��Ƿ���������
     * 
     * @return
     */
    protected boolean isInTransaction() {
        return this.transactionContext.get() != null;
    }


    /**
     * �ύ���񣬽������ڷ��͵���Ϣ�־û����˷���������beginTransaction֮�����
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
     * �ع������������͵��κ���Ϣ���˷���������beginTransaction֮�����
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
            // ����������ڣ���ʹ����һ�η�����Ϣʱѡ���broker
            if (this.isInTransaction()) {
                final LastSentInfo info = this.lastSentInfo.get();
                if (info != null) {
                    serverUrl = info.serverUrl;
                    // ѡ���broker�ڵ�ĳ������
                    partition =
                            this.producerZooKeeper.selectPartition(topic, message, this.partitionSelector, serverUrl);
                    if (partition == null) {
                        // û�п��÷������׳��쳣
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
                // ��һ�η��ͣ���Ҫ��������
                this.beforeSendMessageFirstTime(serverUrl);
            }

            final int flag = MessageFlagUtils.getFlag(message);
            final PutCommand putCommand =
                    new PutCommand(topic, partition.getPartition(), encodedData, this.getTransactionId(), flag,
                        OpaqueGenerator.getNextOpaque());
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
            // �ɹ���������Ϣid����Ϣid�ɷ���������
            MessageAccessor.setId(message, Long.parseLong(tmps[0]));
            final Partition serverPart = new Partition(partition.getBrokerId(), Integer.parseInt(tmps[1]));
            MessageAccessor.setPartition(message, serverPart);
            // ��¼���η�����Ϣ��������������Ϣ
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


    /**
     * ����Ϣ���Ժ���Ϣpayload������ṹ���£�</br></br> 0����1������attribute + payload
     * 
     * @param message
     * @return
     */
    public static byte[] encodeData(final Message message) {
        final byte[] payload = message.getData();
        final String attribute = message.getAttribute();
        byte[] attrData = null;
        if (attribute != null) {
            attrData = ByteUtils.getBytes(attribute);
        }
        else {
            // attribute������һ��Ϊnull��
            return payload;
        }

        final int attrLen = attrData == null ? 0 : attrData.length;
        final ByteBuffer buffer = ByteBuffer.allocate(4 + attrLen + payload.length);
        if (attribute != null) {
            buffer.putInt(attrLen);
            if (attrData != null) {
                buffer.put(attrData);
            }
        }

        buffer.put(payload);
        return buffer.array();
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
            final byte[] encodedData = SimpleMessageProducer.encodeData(message);
            final PutCommand putCommand =
                    new PutCommand(topic, partition.getPartition(), encodedData, this.getTransactionId(), flag,
                        OpaqueGenerator.getNextOpaque());
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