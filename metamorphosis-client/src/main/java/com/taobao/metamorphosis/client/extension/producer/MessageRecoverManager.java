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

import java.io.IOException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.Shutdownable;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 消息暂存和recover管理器的抽象
 * 
 * @author 无花
 * @since 2011-10-27 下午3:34:12
 */

public interface MessageRecoverManager extends Shutdownable {

    /**
     * 全部恢复
     */
    public void recover();


    /**
     * 触发恢复一个主题一个分区的消息
     * 
     * @param topic
     * @param partition
     * @param recoverer
     *            恢复出来的消息的处理器
     * @return 是否真正提交了恢复任务
     * */
    public boolean recover(final String topic, final Partition partition, final MessageRecoverer recoverer);


    /**
     * 存入消息
     * 
     * @param message
     * @param partition
     * @throws IOException
     */
    public void append(Message message, Partition partition) throws IOException;


    /**
     * 消息条数
     * 
     * @param topic
     * @param partition
     * @return
     */
    public int getMessageCount(String topic, Partition partition);


    /**
     * 设置如何恢复消息的处理器
     * 
     * @param recoverer
     */
    public void setMessageRecoverer(MessageRecoverer recoverer);

    /**
     * 指定消息如何recover
     * 
     * @author wuhua
     * 
     */
    public static interface MessageRecoverer {
        /**
         * recover出来的消息如何处理
         * 
         * @param msg
         * @throws Exception
         */
        public void handle(Message msg) throws Exception;
    }
}