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
package com.taobao.metamorphosis.server;

import javax.transaction.xa.XAException;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.exception.MetamorphosisException;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;


/**
 * meta的协议处理接口，封装meta的核心逻辑
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public interface CommandProcessor extends Service {

    public void processPutCommand(final PutCommand request, final SessionContext sessionContext, final PutCallback cb)
            throws Exception;


    public ResponseCommand processGetCommand(GetCommand request, final SessionContext ctx);


    /**
     * Under conditions that cannot use notify-remoting directly.
     */
    public ResponseCommand processGetCommand(GetCommand request, final SessionContext ctx, final boolean zeroCopy);


    public ResponseCommand processOffsetCommand(OffsetCommand request, final SessionContext ctx);


    public void processQuitCommand(QuitCommand request, final SessionContext ctx);


    public ResponseCommand processVesionCommand(VersionCommand request, final SessionContext ctx);


    public ResponseCommand processStatCommand(StatsCommand request, final SessionContext ctx);


    public void removeTransaction(final XATransactionId xid);


    public Transaction getTransaction(final SessionContext context, final TransactionId xid)
            throws MetamorphosisException, XAException;


    public void forgetTransaction(final SessionContext context, final TransactionId xid) throws Exception;


    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception;


    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase)
            throws Exception;


    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception;


    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds)
            throws Exception;


    public TransactionId[] getPreparedTransactions(final SessionContext context, String uniqueQualifier)
            throws Exception;

    // public void setTransactionTimeout(final SessionContext ctx, final
    // TransactionId xid, int seconds) throws Exception;

}