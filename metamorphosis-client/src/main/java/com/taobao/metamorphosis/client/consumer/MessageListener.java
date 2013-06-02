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

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;


/**
 * 异步消息监听器
 * 
 * @author boyan
 * @Date 2011-4-23
 * 
 */
public interface MessageListener {
    /**
     * 接收到消息，只有messages不为空并且不为null的情况下会触发此方法
     * 
     * @param messages
     *            TODO 拼写错误，应该是单数，暂时将错就错吧
     */
    public void recieveMessages(Message message) throws InterruptedException;


    /**
     * 处理消息的线程池
     * 
     * @return
     */
    public Executor getExecutor();
}