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

import java.io.Serializable;


/**
 * 订阅关系对象
 * 
 * @author boyan
 * @Date 2011-5-18
 * 
 */
class Subscription implements Serializable {
    static final long serialVersionUID = -1L;
    private String topic;

    private int maxSize;

    private transient MessageListener messageListener;


    public Subscription(final String topic, final int maxSize, final MessageListener messageListener) {
        super();
        this.topic = topic;
        this.maxSize = maxSize;
        this.messageListener = messageListener;
    }


    public Subscription() {
        super();
    }


    public String getTopic() {
        return this.topic;
    }


    public void setTopic(final String topic) {
        this.topic = topic;
    }


    public int getMaxSize() {
        return this.maxSize;
    }


    public void setMaxSize(final int maxSize) {
        this.maxSize = maxSize;
    }


    public MessageListener getMessageListener() {
        return this.messageListener;
    }


    public void setMessageListener(final MessageListener messageListener) {
        this.messageListener = messageListener;
    }

}