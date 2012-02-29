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
package com.taobao.metamorphosis.storm.spout;

import java.util.concurrent.CountDownLatch;

import com.taobao.metamorphosis.Message;


/**
 * Meta消息的包装类，关联一个CountDownLatch
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-8
 * 
 */
public final class MetaMessageWrapper {

    public final Message message;
    public final CountDownLatch latch;
    public volatile boolean success = false;


    public MetaMessageWrapper(final Message message) {
        super();
        this.message = message;
        this.latch = new CountDownLatch(1);
    }

}