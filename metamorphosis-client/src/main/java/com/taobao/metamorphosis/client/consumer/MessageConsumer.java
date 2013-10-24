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
package com.taobao.metamorphosis.client.consumer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.client.Shutdownable;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 消息消费者，线程安全，推荐复用
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public interface MessageConsumer extends Shutdownable {

    /**
     * 获取指定topic和分区下面的消息，默认超时10秒
     * 
     * @param topic
     * @param partition
     * @return 消息迭代器，可能为null
     */
    public MessageIterator get(String topic, Partition partition, long offset, int maxSize) throws MetaClientException,
    InterruptedException;


    /**
     * 获取指定topic和分区下面的消息，在指定时间内没有返回则抛出异常
     * 
     * @param topic
     * @param partition
     * @param timeout
     * @param timeUnit
     * @return 消息迭代器，可能为null
     * @throws TimeoutException
     */
    public MessageIterator get(String topic, Partition partition, long offset, int maxSize, long timeout,
            TimeUnit timeUnit) throws MetaClientException, InterruptedException;


    /**
     * 订阅指定的消息，传入MessageListener，当有消息达到的时候主动通知MessageListener，请注意，
     * 调用此方法并不会使订阅关系立即生效， 只有在调用complete方法后才生效，此方法可做链式调用
     * 
     * @param topic
     *            订阅的topic
     * @param maxSize
     *            订阅每次接收的最大数据大小
     * @param messageListener
     *            消息监听器
     */
    public MessageConsumer subscribe(String topic, int maxSize, MessageListener messageListener)
            throws MetaClientException;


    /**
     * 订阅指定的消息，传入MessageListener和ConsumerMessageFilter，
     * 当有消息到达并且ConsumerMessageFilter
     * #accept返回true的时候,主动通知MessageListener该条消息，请注意， 调用此方法并不会使订阅关系立即生效，
     * 只有在调用complete方法后才生效，此方法可做链式调用
     * 
     * @param topic
     *            订阅的topic
     * @param maxSize
     *            订阅每次接收的最大数据大小
     * @param messageListener
     * @param ConsumerMessageFilter
     *            message filter 消息监听器
     */
    public MessageConsumer subscribe(String topic, int maxSize, MessageListener messageListener,
            ConsumerMessageFilter consumerMessageFilter) throws MetaClientException;


    /**
     * 批量订阅消息,请注意，调用此方法并不会使订阅关系立即生效，只有在调用complete方法后才生效。
     * 
     * @param subscriptions
     */
    public void setSubscriptions(Collection<Subscription> subscriptions) throws MetaClientException;


    /**
     * 使得已经订阅的topic生效,此方法仅能调用一次,再次调用无效并将抛出异常
     */
    public void completeSubscribe() throws MetaClientException;


    /**
     * 返回此消费者使用的offset存储器，可共享给其他消费者
     * 
     * @return
     */
    public OffsetStorage getOffsetStorage();


    /**
     * 停止消费者
     */
    @Override
    public void shutdown() throws MetaClientException;


    /**
     * 返回消费者配置
     * 
     * @return
     */
    public ConsumerConfig getConsumerConfig();

    /**
     * Returns current RejectConsumptionHandler
     *
     * @return
     */
    public RejectConsumptionHandler getRejectConsumptionHandler();

    /**
     * Sets RejectConsumptionHandler for this consumer.
     *
     * @param rejectConsumptionHandler
     */
    public void setRejectConsumptionHandler(RejectConsumptionHandler rejectConsumptionHandler);
}