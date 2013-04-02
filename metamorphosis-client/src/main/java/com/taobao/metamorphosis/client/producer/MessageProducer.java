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

import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.Shutdownable;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 消息生产者，线程安全，推荐复用
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public interface MessageProducer extends Shutdownable {

    /**
     * 发布topic，以便producer从zookeeper获取broker列表并连接，在发送消息前必须先调用此方法
     * 
     * @param topic
     */
    public void publish(String topic);


    /**
     * 设置发送消息的默认topic，当发送的message的topic没有找到可用broker和分区的时候，选择这个默认topic指定的broker发送
     * 。调用本方法会自动publish此topic。
     * 
     * @param topic
     */
    public void setDefaultTopic(String topic);


    /**
     * 发送消息
     * 
     * @param message
     *            消息对象
     * 
     * @return 发送结果
     * @throws MetaClientException
     *             客户端异常
     * @throws InterruptedException
     *             响应中断
     */
    public SendResult sendMessage(Message message) throws MetaClientException, InterruptedException;


    /**
     * 异步发送消息，在指定时间内回调callback,此模式下无法使用事务
     * 
     * @param message
     * @param cb
     * @param time
     * @param unit
     * @since 1.4
     */
    public void sendMessage(Message message, SendMessageCallback cb, long time, TimeUnit unit);


    /**
     * 异步发送消息，在默认时间内（3秒）回调callback，此模式下无法使用事务
     * 
     * @param message
     * @param cb
     * @since 1.4
     */
    public void sendMessage(Message message, SendMessageCallback cb);


    /**
     * 发送消息,如果超出指定的时间内没有返回，则抛出异常
     * 
     * @param message
     *            消息对象
     * @param timeout
     *            超时时间
     * @param unit
     *            超时的时间单位
     * @return 发送结果
     * @throws MetaClientException
     *             客户端异常
     * @throws InterruptedException
     *             响应中断
     */
    public SendResult sendMessage(Message message, long timeout, TimeUnit unit) throws MetaClientException,
    InterruptedException;


    /**
     * 关闭生产者，释放资源
     */
    @Override
    public void shutdown() throws MetaClientException;


    /**
     * 返回本生产者的分区选择器
     * 
     * @return
     */
    public PartitionSelector getPartitionSelector();


    /**
     * 返回本生产者发送消息是否有序,这里的有序是指发往同一个partition的消息有序。此方法已经废弃，总是返回false
     * 
     * @return true表示有序
     */
    @Deprecated
    public boolean isOrdered();


    /**
     * 开启一个事务并关联到当前线程，在事务内发送的消息将作为一个单元提交给服务器，要么全部发送成功，要么全部失败
     * 
     * @throws MetaClientException
     *             如果已经处于事务中，则抛出TransactionInProgressException异常
     */
    public void beginTransaction() throws MetaClientException;


    /**
     * 设置事务超时时间，从事务开始计时，如果超过设定时间还没有提交或者回滚，则服务端将无条件回滚该事务。
     * 
     * @param seconds
     *            事务超时时间，单位：秒
     * @throws MetaClientException
     * @see #beginTransaction()
     * @see #rollback()
     * @see #commit()
     */
    public void setTransactionTimeout(int seconds) throws MetaClientException;


    /**
     * Set transaction command request timeout.default is five seconds.
     * 
     * @param time
     * @param timeUnit
     */
    public void setTransactionRequestTimeout(long time, TimeUnit timeUnit);


    /**
     * 返回当前设置的事务超时时间，默认为0,表示永不超时
     * 
     * @return 事务超时时间，单位：秒
     * @throws MetaClientException
     */
    public int getTransactionTimeout() throws MetaClientException;


    /**
     * 回滚当前事务内所发送的任何消息，此方法仅能在beginTransaction之后调用
     * 
     * @throws MetaClientException
     * @see #beginTransaction()
     */
    public void rollback() throws MetaClientException;


    /**
     * 提交当前事务，将事务内发送的消息持久化，此方法仅能在beginTransaction之后调用
     * 
     * @see #beginTransaction()
     * @throws MetaClientException
     */
    public void commit() throws MetaClientException;

}