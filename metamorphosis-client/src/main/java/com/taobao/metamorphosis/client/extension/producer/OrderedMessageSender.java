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
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 
 * @author 无花
 * @since 2011-8-9 下午6:12:01
 */

class OrderedMessageSender {
    private final OrderedMessageProducer producer;


    OrderedMessageSender(OrderedMessageProducer producer) {
        this.producer = producer;
    }


    SendResult sendMessage(Message message, long timeout, TimeUnit unit) throws MetaClientException,
            InterruptedException {
        int maxRecheck = 3;
        int check = 1;
        for (;;) {
            SelectPartitionResult result = this.trySelectPartition(message);

            // 可用分区不正常
            if (!result.isPartitionWritable()) {
                return this.producer.saveMessageToLocal(message, result.getSelectedPartition(), timeout, unit);
            }
            // 可用分区正常
            else {
                int localMessageCount =
                        this.producer.getLocalMessageCount(message.getTopic(), result.getSelectedPartition());
                if (localMessageCount > 0) {
                    this.producer.tryRecoverMessage(message.getTopic(), result.getSelectedPartition());
                }

                if (localMessageCount <= 0) {
                    // 本地存的消息条数为空,本次消息发送到服务端
                    return this.producer.sendMessageToServer(message, timeout, unit, true);
                }
                else if (localMessageCount > 0 && localMessageCount <= 20) {
                    // 本地存的消息只有少量几条,停顿一下等待本地消息被恢复,`再继续检查状态,
                    // 最多检查maxRecheck次 本地消息还没被恢复完,存本地并退出.
                    if (check >= maxRecheck) {
                        return this.producer.saveMessageToLocal(message, result.getSelectedPartition(), timeout, unit);
                    }
                    Thread.sleep(100L);

                }
                else {
                    // 本地存的消息还有很多,继续写本地
                    return this.producer.saveMessageToLocal(message, result.getSelectedPartition(), timeout, unit);
                }

            }
            check++;
        }// end for

    }


    private SelectPartitionResult trySelectPartition(Message message) throws MetaClientException {
        SelectPartitionResult result = new SelectPartitionResult();
        try {
            Partition partition = this.producer.selectPartition(message);
            if (partition == null) {
                throw new MetaClientException("selected null partition");
            }
            result.setSelectedPartition(partition);
            result.setPartitionWritable(true);
        }
        catch (AvailablePartitionNumException e) {
            String msg = e.getMessage();
            String partitionStr = msg.substring(msg.indexOf("[") + 1, msg.indexOf("]"));
            result.setSelectedPartition(new Partition(partitionStr));
            result.setPartitionWritable(false);
        }
        catch (MetaClientException e) {
            throw e;
        }
        return result;
    }

    private static class SelectPartitionResult {
        private boolean partitionWritable;
        private Partition selectedPartition;


        public boolean isPartitionWritable() {
            return this.partitionWritable;
        }


        public void setPartitionWritable(boolean partitionWritable) {
            this.partitionWritable = partitionWritable;
        }


        public Partition getSelectedPartition() {
            return this.selectedPartition;
        }


        public void setSelectedPartition(Partition selectedPartition) {
            this.selectedPartition = selectedPartition;
        }

    }

}