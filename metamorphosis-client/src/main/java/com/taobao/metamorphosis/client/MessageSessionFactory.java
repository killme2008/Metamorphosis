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
package com.taobao.metamorphosis.client;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 消息会话工厂，meta客户端的主接口,推荐一个应用只使用一个MessageSessionFactory
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
public interface MessageSessionFactory extends Shutdownable {

    /**
     * 关闭工厂
     * 
     * @throws MetaClientException
     */
    @Override
    public void shutdown() throws MetaClientException;


    /**
     * 创建消息生产者
     * 
     * @param partitionSelector
     *            分区选择器
     * @return
     */
    public MessageProducer createProducer(PartitionSelector partitionSelector);


    /**
     * 创建消息生产者，默认使用轮询分区选择器
     * 
     * @return
     */
    public MessageProducer createProducer();


    /**
     * 创建消息生产者，默认使用轮询分区选择器。本方法已经废弃，请勿使用，不排除在未来某个版本删除。
     * 
     * @param ordered
     *            是否有序，true为有序，如果有序，则消息按照发送顺序保存在MQ server
     * @return
     */
    @Deprecated
    public MessageProducer createProducer(boolean ordered);


    /**
     * 创建消息生产者,本方法已经废弃，请勿使用，不排除在未来某个版本删除。
     * 
     * @param partitionSelector
     *            分区选择器
     * @param ordered
     *            是否有序，true为有序，如果有序，则消息按照发送顺序保存在MQ server
     * @return
     */
    @Deprecated
    public MessageProducer createProducer(PartitionSelector partitionSelector, boolean ordered);


    /**
     * 创建消息消费者，默认将offset存储在zk
     * 
     * @param consumerConfig
     *            消费者配置
     * @return
     */
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig);


    /**
     * Get statistics information from all brokers in this session factory.
     * 
     * @return statistics result
     * @since 1.4.2
     */
    public Map<InetSocketAddress, StatsResult> getStats() throws InterruptedException;


    /**
     * Get item statistics information from all brokers in this session factory.
     * 
     * @param item
     *            stats item,could be "topics","realtime","offsets" or a special
     *            topic
     * @return statistics result
     * @since 1.4.2
     */
    public Map<InetSocketAddress, StatsResult> getStats(String item) throws InterruptedException;


    /**
     * Get statistics information from special broker.If the broker is not
     * connected in this session factory,it will return null.
     * 
     * @param target
     *            stats broker
     * @return statistics result
     * @since 1.4.2
     */
    public StatsResult getStats(InetSocketAddress target) throws InterruptedException;


    /**
     * Get item statistics information from special broker.If the broker is not
     * connected in this session factory,it will return null.
     * 
     * @param target
     *            stats broker
     * @param item
     *            stats item,could be "topics","realtime","offsets" or a special
     *            topic
     * @return statistics result
     * @since 1.4.2
     */
    public StatsResult getStats(InetSocketAddress target, String item) throws InterruptedException;


    /**
     * 创建消息消费者，使用指定的offset存储器
     * 
     * @param consumerConfig
     *            消费者配置
     * @param offsetStorage
     *            offset存储器
     * @return
     */
    public MessageConsumer createConsumer(ConsumerConfig consumerConfig, OffsetStorage offsetStorage);


    /**
     * Get partitions list for topic
     * 
     * @param topic
     * @return partitions list
     */
    public List<Partition> getPartitionsForTopic(String topic);


    /**
     * Returns a topic browser to iterate all messages under the topic from all
     * alive brokers.
     * 
     * @param topic
     *            the topic
     * @param maxSize
     *            fetch batch size in bytes.
     * @param timeout
     *            timeout value to fetch messages.
     * @param timeUnit
     *            timeout value unit.
     * @since 1.4.5
     * @return topic browser
     */
    public TopicBrowser createTopicBrowser(String topic, int maxSize, long timeout, TimeUnit timeUnit);


    /**
     * Returns a topic browser to iterate all messages under the topic from all
     * alive brokers.
     * 
     * @param topic
     *            the topic
     * @since 1.4.5
     * @return topic browser
     * @see #createTopicBrowser(String, int, long, TimeUnit)
     */
    public TopicBrowser createTopicBrowser(String topic);

}