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

import java.io.IOException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.Shutdownable;


/**
 * 消费端的Recover管理器
 * 
 * @author 无花
 * @since 2011-10-31 下午3:40:04
 */

public interface RecoverManager extends Shutdownable {
    /**
     * 是否已经启动
     * 
     * @return
     */
    public boolean isStarted();


    /**
     * 启动recover
     * 
     * @param metaClientConfig
     */
    public void start(MetaClientConfig metaClientConfig);


    /**
     * 存入一个消息
     * 
     * @param group
     * @param message
     * @throws IOException
     */
    public void append(String group, Message message) throws IOException;
}