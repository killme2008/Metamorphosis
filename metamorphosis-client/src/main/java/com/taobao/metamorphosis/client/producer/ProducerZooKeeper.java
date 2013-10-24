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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.ZkClientChangedListener;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ThreadUtils;
import com.taobao.metamorphosis.utils.ZkUtils;


/**
 * Producer和zk的交互
 * 
 * @author boyan
 * @Date 2011-4-26
 */
public class ProducerZooKeeper implements ZkClientChangedListener {

    private final RemotingClientWrapper remotingClient;

    private final ConcurrentHashMap<String, FutureTask<BrokerConnectionListener>> topicConnectionListeners =
            new ConcurrentHashMap<String, FutureTask<BrokerConnectionListener>>();

    private final MetaClientConfig metaClientConfig;

    private ZkClient zkClient;

    private final MetaZookeeper metaZookeeper;

    /**
     * 默认topic，当查找分区没有找到可用分区的时候，发送到此topic下的broker
     */
    private String defaultTopic;

    static final Log log = LogFactory.getLog(ProducerZooKeeper.class);

    public static class BrokersInfo {
        final Map<Integer/* broker id */, String/* server url */> oldBrokerStringMap;
        final Map<String/* topic */, List<Partition>/* partition list */> oldTopicPartitionMap;


        public BrokersInfo(final Map<Integer, String> oldBrokerStringMap,
                final Map<String, List<Partition>> oldTopicPartitionMap) {
            super();
            this.oldBrokerStringMap = oldBrokerStringMap;
            this.oldTopicPartitionMap = oldTopicPartitionMap;
        }

    }

    /**
     * When producer broker list is changed, it will notify the this listener.
     * 
     * @author apple
     * 
     */
    public static interface BrokerChangeListener {
        /**
         * called when broker list changed.
         * 
         * @param topic
         */
        public void brokersChanged(String topic);
    }

    private final ConcurrentHashMap<String, CopyOnWriteArraySet<BrokerChangeListener>> brokerChangeListeners =
            new ConcurrentHashMap<String, CopyOnWriteArraySet<BrokerChangeListener>>();


    public void onBrokerChange(String topic, BrokerChangeListener listener) {
        CopyOnWriteArraySet<BrokerChangeListener> set = this.getListenerList(topic);
        set.add(listener);
    }


    public void deregisterBrokerChangeListener(String topic, BrokerChangeListener listener) {
        CopyOnWriteArraySet<BrokerChangeListener> set = this.getListenerList(topic);
        set.remove(listener);
    }


    public void notifyBrokersChange(String topic) {
        for (final BrokerChangeListener listener : this.getListenerList(topic)) {
            try {
                listener.brokersChanged(topic);
            }
            catch (Exception e) {
                log.error("Notify brokers changed failed", e);
            }
        }
    }


    private CopyOnWriteArraySet<BrokerChangeListener> getListenerList(String topic) {
        CopyOnWriteArraySet<BrokerChangeListener> set = this.brokerChangeListeners.get(topic);
        if (set == null) {
            set = new CopyOnWriteArraySet<ProducerZooKeeper.BrokerChangeListener>();
            CopyOnWriteArraySet<BrokerChangeListener> oldSet = this.brokerChangeListeners.putIfAbsent(topic, set);
            if (oldSet != null) {
                set = oldSet;
            }
        }
        return set;
    }

    final class BrokerConnectionListener implements IZkChildListener {

        final Lock lock = new ReentrantLock();
        volatile BrokersInfo brokersInfo = new BrokersInfo(new TreeMap<Integer, String>(),
            new HashMap<String, List<Partition>>());

        final String topic;

        final Set<Object> references = Collections.synchronizedSet(new HashSet<Object>());


        public BrokerConnectionListener(final String topic) {
            super();
            this.topic = topic;
        }


        void dispose() {
            final String partitionPath = ProducerZooKeeper.this.metaZookeeper.brokerTopicsPubPath + "/" + this.topic;
            ProducerZooKeeper.this.zkClient.unsubscribeChildChanges(partitionPath, this);
        }


        /**
         * 处理broker增减
         */
        @Override
        public void handleChildChange(final String parentPath, final List<String> currentChilds) throws Exception {
            this.syncedUpdateBrokersInfo();
        }


        void syncedUpdateBrokersInfo() throws NotifyRemotingException, InterruptedException {
            this.lock.lock();
            try {

                final Map<Integer, String> newBrokerStringMap =
                        ProducerZooKeeper.this.metaZookeeper.getMasterBrokersByTopic(this.topic);
                final List<String> topics = new ArrayList<String>(1);
                topics.add(this.topic);
                final Map<String, List<Partition>> newTopicPartitionMap =
                        ProducerZooKeeper.this.metaZookeeper.getPartitionsForTopicsFromMaster(topics);

                log.warn("Begin receiving broker changes for topic " + this.topic + ",broker ids:"
                        + newTopicPartitionMap);
                final boolean changed = !this.brokersInfo.oldBrokerStringMap.equals(newBrokerStringMap);

                // Close old brokers;
                for (final Map.Entry<Integer, String> oldEntry : this.brokersInfo.oldBrokerStringMap.entrySet()) {
                    final String oldBrokerString = oldEntry.getValue();
                    ProducerZooKeeper.this.remotingClient.closeWithRef(oldBrokerString, this, false);
                    log.warn("Closed " + oldBrokerString);
                }
                // Connect to new brokers
                for (final Map.Entry<Integer, String> newEntry : newBrokerStringMap.entrySet()) {
                    final String newBrokerString = newEntry.getValue();
                    ProducerZooKeeper.this.remotingClient.connectWithRef(newBrokerString, this);
                    try {
                        ProducerZooKeeper.this.remotingClient.awaitReadyInterrupt(newBrokerString, 10000);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Connecting to broker is interrupted", e);
                    }
                    log.warn("Connected to " + newBrokerString);
                }

                // Set the new brokers info.
                this.brokersInfo = new BrokersInfo(newBrokerStringMap, newTopicPartitionMap);
                if (changed) {
                    ProducerZooKeeper.this.notifyBrokersChange(this.topic);
                }
                log.warn("End receiving broker changes for topic " + this.topic);
            }
            finally {
                this.lock.unlock();
            }
        }
    }


    public ProducerZooKeeper(final MetaZookeeper metaZookeeper, final RemotingClientWrapper remotingClient,
            final ZkClient zkClient, final MetaClientConfig metaClientConfig) {
        super();
        this.metaZookeeper = metaZookeeper;
        this.remotingClient = remotingClient;
        this.zkClient = zkClient;
        this.metaClientConfig = metaClientConfig;
    }


    public void publishTopic(final String topic, final Object ref) {
        if (this.topicConnectionListeners.get(topic) != null) {
            this.addRef(topic, ref);
            return;
        }
        final FutureTask<BrokerConnectionListener> task =
                new FutureTask<BrokerConnectionListener>(new Callable<BrokerConnectionListener>() {
                    @Override
                    public BrokerConnectionListener call() throws Exception {
                        final BrokerConnectionListener listener = new BrokerConnectionListener(topic);
                        if (ProducerZooKeeper.this.zkClient != null) {
                            ProducerZooKeeper.this.publishTopicInternal(topic, listener);
                        }
                        listener.references.add(ref);
                        return listener;
                    }

                });

        final FutureTask<BrokerConnectionListener> existsTask = this.topicConnectionListeners.putIfAbsent(topic, task);
        if (existsTask == null) {
            task.run();
        }
        else {
            this.addRef(topic, ref);
        }
    }


    private void addRef(final String topic, final Object ref) {
        BrokerConnectionListener listener = this.getBrokerConnectionListener(topic);
        if (!listener.references.contains(ref)) {
            listener.references.add(ref);
        }
    }


    public void unPublishTopic(String topic, Object ref) {
        BrokerConnectionListener listener = this.getBrokerConnectionListener(topic);
        if (listener != null) {
            synchronized (listener.references) {
                if (this.getBrokerConnectionListener(topic) == null) {
                    return;
                }
                listener.references.remove(ref);
                if (listener.references.isEmpty()) {
                    this.topicConnectionListeners.remove(topic);
                    listener.dispose();
                }
            }
        }
    }


    private void publishTopicInternal(final String topic, final BrokerConnectionListener listener) throws Exception,
    NotifyRemotingException, InterruptedException {
        final String partitionPath = this.metaZookeeper.brokerTopicsPubPath + "/" + topic;
        ZkUtils.makeSurePersistentPathExists(ProducerZooKeeper.this.zkClient, partitionPath);
        ProducerZooKeeper.this.zkClient.subscribeChildChanges(partitionPath, listener);
        // 第一次要同步等待就绪
        listener.syncedUpdateBrokersInfo();
    }


    BrokerConnectionListener getBrokerConnectionListener(final String topic) {
        final FutureTask<BrokerConnectionListener> task = this.topicConnectionListeners.get(topic);
        if (task != null) {
            try {
                return task.get();
            }
            catch (final ExecutionException e) {
                throw ThreadUtils.launderThrowable(e.getCause());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }


    /**
     * 根据topic查找服务器url列表
     * 
     * @param topic
     * @return
     */
    Set<String> getServerUrlSetByTopic(final String topic) {
        final BrokerConnectionListener brokerConnectionListener = this.getBrokerConnectionListener(topic);
        if (brokerConnectionListener != null) {
            final BrokersInfo info = brokerConnectionListener.brokersInfo;

            final Map<Integer/* broker id */, String/* server url */> brokerStringMap = info.oldBrokerStringMap;
            final Map<String/* topic */, List<Partition>/* partition list */> topicPartitionMap =
                    info.oldTopicPartitionMap;
            final List<Partition> plist = topicPartitionMap.get(topic);
            if (plist != null) {
                final Set<String> result = new HashSet<String>();
                for (final Partition partition : plist) {
                    final int brokerId = partition.getBrokerId();
                    final String url = brokerStringMap.get(brokerId);
                    if (url != null) {
                        result.add(url);
                    }
                }
                return result;
            }
        }
        return Collections.emptySet();
    }


    /**
     * 设置默认topic并发布
     * 
     * @param topic
     */
    public synchronized void setDefaultTopic(final String topic, Object ref) {
        if (this.defaultTopic != null && !this.defaultTopic.equals(topic)) {
            throw new IllegalStateException("Default topic has been setup already:" + this.defaultTopic);
        }
        this.defaultTopic = topic;
        this.publishTopic(topic, ref);
    }


    /**
     * 
     * 选择指定broker内的某个分区，用于事务内发送消息，此方法仅用于local transaction
     * 
     * @param topic
     * @return
     */
    Partition selectPartition(final String topic, final Message msg, final PartitionSelector selector,
            final String serverUrl) throws MetaClientException {
        boolean oldReadOnly = msg.isReadOnly();
        try {
            msg.setReadOnly(true);
            final BrokerConnectionListener brokerConnectionListener = this.getBrokerConnectionListener(topic);
            if (brokerConnectionListener != null) {
                final BrokersInfo brokersInfo = brokerConnectionListener.brokersInfo;
                final List<Partition> partitions = brokersInfo.oldTopicPartitionMap.get(topic);
                final Map<Integer/* broker id */, String/* server url */> brokerStringMap =
                        brokersInfo.oldBrokerStringMap;
                // 过滤特定broker的分区列表
                final List<Partition> partitionsForSelect = new ArrayList<Partition>();
                for (final Partition partition : partitions) {
                    if (serverUrl.equals(brokerStringMap.get(partition.getBrokerId()))) {
                        partitionsForSelect.add(partition);
                    }
                }
                return selector.getPartition(topic, partitionsForSelect, msg);
            }
            else {
                return this.selectDefaultPartition(topic, msg, selector, serverUrl);
            }
        }
        finally {
            msg.setReadOnly(oldReadOnly);
        }
    }


    /**
     * 根据partition寻找broker url
     * 
     * @param topic
     * @param message
     * @return 选中的broker的url
     */
    public String selectBroker(final String topic, final Partition partition) {
        if (this.metaClientConfig.getServerUrl() != null) {
            return this.metaClientConfig.getServerUrl();
        }
        if (partition != null) {
            final BrokerConnectionListener brokerConnectionListener = this.getBrokerConnectionListener(topic);
            if (brokerConnectionListener != null) {
                final BrokersInfo brokersInfo = brokerConnectionListener.brokersInfo;
                return brokersInfo.oldBrokerStringMap.get(partition.getBrokerId());
            }
            else {
                return this.selectDefaultBroker(topic, partition);
            }
        }
        return null;
    }


    /**
     * 从defaultTopic中选择broker
     * 
     * @param topic
     * @param partition
     * @return
     */
    private String selectDefaultBroker(final String topic, final Partition partition) {
        if (this.defaultTopic == null) {
            return null;
        }
        final BrokerConnectionListener brokerConnectionListener = this.getBrokerConnectionListener(this.defaultTopic);
        if (brokerConnectionListener != null) {
            final BrokersInfo brokersInfo = brokerConnectionListener.brokersInfo;
            return brokersInfo.oldBrokerStringMap.get(partition.getBrokerId());
        }
        else {
            return null;
        }
    }


    /**
     * 根据topic和message选择分区
     * 
     * @param topic
     * @param message
     * @return 选中的分区
     */
    public Partition selectPartition(final String topic, final Message message,
            final PartitionSelector partitionSelector) throws MetaClientException {
        boolean oldReadOnly = message.isReadOnly();
        try {
            message.setReadOnly(true);
            if (this.metaClientConfig.getServerUrl() != null) {
                return Partition.RandomPartiton;
            }
            final BrokerConnectionListener brokerConnectionListener = this.getBrokerConnectionListener(topic);
            if (brokerConnectionListener != null) {
                final BrokersInfo brokersInfo = brokerConnectionListener.brokersInfo;
                return partitionSelector.getPartition(topic, brokersInfo.oldTopicPartitionMap.get(topic), message);
            }
            else {
                return this.selectDefaultPartition(topic, message, partitionSelector, null);
            }
        }
        finally {
            message.setReadOnly(oldReadOnly);
        }
    }


    private Partition selectDefaultPartition(final String topic, final Message message,
            final PartitionSelector partitionSelector, final String serverUrl) throws MetaClientException {
        if (this.defaultTopic == null) {
            return null;
        }
        final BrokerConnectionListener brokerConnectionListener = this.getBrokerConnectionListener(this.defaultTopic);
        if (brokerConnectionListener != null) {
            final BrokersInfo brokersInfo = brokerConnectionListener.brokersInfo;
            if (serverUrl == null) {
                return partitionSelector.getPartition(this.defaultTopic,
                    brokersInfo.oldTopicPartitionMap.get(this.defaultTopic), message);
            }
            else {
                final List<Partition> partitions = brokersInfo.oldTopicPartitionMap.get(this.defaultTopic);
                final Map<Integer/* broker id */, String/* server url */> brokerStringMap =
                        brokersInfo.oldBrokerStringMap;
                // 过滤特定broker的分区列表
                final List<Partition> partitionsForSelect = new ArrayList<Partition>();
                for (final Partition partition : partitions) {
                    if (serverUrl.equals(brokerStringMap.get(partition.getBrokerId()))) {
                        partitionsForSelect.add(partition);
                    }
                }
                return partitionSelector.getPartition(this.defaultTopic, partitionsForSelect, message);
            }
        }
        else {
            return null;
        }
    }


    @Override
    public void onZkClientChanged(final ZkClient newClient) {
        this.zkClient = newClient;
        try {
            for (final String topic : this.topicConnectionListeners.keySet()) {
                log.info("re-publish topic to zk,topic=" + topic);
                this.publishTopicInternal(topic, this.getBrokerConnectionListener(topic));
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (final Exception e) {
            log.error("重新设置zKClient失败", e);
        }
    }

}