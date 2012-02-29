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
package com.taobao.metamorphosis.client.extension.producer;

import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.producer.MessageProducer;


/**
 * <pre>
 * 异步发送消息的生产者.
 * 
 * 使用场景:
 *      对于发送可靠性要求不那么高,但要求提高发送效率和降低对宿主应用的影响，提高宿主应用的稳定性.
 *      例如,收集日志或用户行为信息等场景.
 * 注意:
 *      发送消息后返回的结果中不包含准确的messageId和offset,这些值都是-1
 * 
 * @author 无花
 * @since 2011-10-21 下午1:42:55
 * </pre>
 */

public interface AsyncMessageProducer extends MessageProducer {

    /**
     * <pre>
     * 异步发送消息.
     * 最大限度的减少对业务主流程的影响,使用者不关心发送成功或失败和所有异常
     * 
     * @param message
     * 
     * </pre>
     */
    public void asyncSendMessage(Message message);


    /**
     * <pre>
     * 异步发送消息. 
     * 最大限度的减少对业务主流程的影响,使用者不关心发送成功或失败和所有异常
     * 
     * @param message
     * @param timeout
     * @param unit
     * </pre>
     */
    public void asyncSendMessage(Message message, long timeout, TimeUnit unit);


    /**
     * 设置发送失败和超过流控消息的处理器,用户可以自己接管这些消息如何处理
     * 
     * @param ignoreMessageProcessor
     */
    public void setIgnoreMessageProcessor(IgnoreMessageProcessor ignoreMessageProcessor);

    /**
     * 用于处理发送失败和超出流控的消息
     * 
     * @author wuhua
     * 
     */
    public interface IgnoreMessageProcessor {
        /**
         * 处理一条消息
         * 
         * @param message
         * @return
         * @throws Exception
         */
        boolean handle(Message message) throws Exception;
    }
}