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
package com.taobao.metamorphosis.server.transaction;

import java.io.IOException;

import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.transaction.store.JournalLocation;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 事务性存储引擎
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-23
 * 
 */
public interface TransactionStore extends Service {

    void prepare(TransactionId txid) throws IOException;


    void commit(TransactionId txid, boolean wasPrepared) throws IOException;


    void rollback(TransactionId txid) throws IOException;


    public void addMessage(final MessageStore store, long msgId, final PutCommand cmd, JournalLocation location)
            throws IOException;


    void recover(TransactionRecoveryListener listener) throws IOException;
}