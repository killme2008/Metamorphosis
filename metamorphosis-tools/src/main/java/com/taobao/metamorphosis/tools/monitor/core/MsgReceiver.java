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
package com.taobao.metamorphosis.tools.monitor.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 代表直连一台broker的消息接收者
 * 
 * @author 无花
 * @since 2011-5-24 上午11:35:48
 */
// 自身维护offset,非线程安全
public class MsgReceiver {

    /** 一台broker上不同分区上的接收offset */
    private final Map<String/* partition */, Long/* offset */> offsetMap = new HashMap<String, Long>();

    final private MessageConsumer consumer;

    final static private String group = "meta-monitor-receive";

    private String serverUrl = StringUtils.EMPTY;
    private final MessageSessionFactory sessionFactory;


    public MsgReceiver(String serverUrl, MonitorConfig monitorConfig) throws MetaClientException {
        this.serverUrl = serverUrl;
        MetaClientConfig metaClientConfig = monitorConfig.metaClientConfigOf(serverUrl);
        this.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        this.consumer = this.sessionFactory.createConsumer(new ConsumerConfig(group));
    }


    /**
     * <pre>
     * 同步的方式接收消息.
     * 期望从partition尾部开始接收(需要把接收的offset设置为上一次成功发送消息后的offset)
     * 异常处理:这里捕获所有异常(Error除外),装入result的形式返回
     * </pre>
     */
    public ReveiceResult get(String topic, Partition partition) {
        long offset = this.getOffset(partition);
        ReveiceResult result = new ReveiceResult(topic, partition, offset, this.serverUrl);
        MessageIterator it = null;
        try {
            int i = 0;
            while ((it = this.consumer.get(topic, partition, offset, 1024 * 1024)) != null) {
                // 防止第一次从0-offset接收太多的数据.
                if (i++ >= 3) {
                    break;
                }
                while (it.hasNext()) {
                    final Message msg = it.next();
                    result.addMessage(msg);
                    // System.out.println("Receive message " + new
                    // String(msg.getData()));
                }
                offset += it.getOffset();
                this.setOffset(partition, offset);
            }
        }
        catch (InvalidMessageException e) {
            result.setException(e);
        }
        catch (MetaClientException e) {
            result.setException(e);
        }
        catch (InterruptedException e) {
            result.setException(e);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            result.setException(e);
        }
        return result;
    }


    public void setOffset(Partition partition, long offset) {
        this.offsetMap.put(partition.toString(), offset);
    }


    public long getOffset(Partition partition) {
        String key = partition.toString();
        Long offset = this.offsetMap.get(key);
        if (offset == null) {
            this.offsetMap.put(key, Long.valueOf(0));
        }
        return this.offsetMap.get(key).longValue();
    }


    public void dispose() {
        try {
            this.sessionFactory.shutdown();
        }
        catch (MetaClientException e) {
            // ignore
        }
    }

}