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
 * Processor过滤器实现，可继承并自定义实现
 * 
 * @author boyan
 * 
 */
public class CommandProcessorFilter implements CommandProcessor {
    private final CommandProcessor next;


    public CommandProcessorFilter(final CommandProcessor next) {
        super();
        this.next = next;
    }


    public CommandProcessor getNext() {
        return this.next;
    }


    @Override
    public void init() {
        this.next.init();
    }


    @Override
    public void dispose() {
        this.next.dispose();
    }


    @Override
    public void processPutCommand(final PutCommand request, final SessionContext sessionContext, final PutCallback cb)
            throws Exception {
        this.next.processPutCommand(request, sessionContext, cb);
    }


    @Override
    public ResponseCommand processGetCommand(final GetCommand request, final SessionContext ctx) {
        return this.next.processGetCommand(request, ctx);
    }


    @Override
    public ResponseCommand processOffsetCommand(final OffsetCommand request, final SessionContext ctx) {
        return this.next.processOffsetCommand(request, ctx);
    }


    @Override
    public void processQuitCommand(final QuitCommand request, final SessionContext ctx) {
        this.next.processQuitCommand(request, ctx);
    }


    @Override
    public ResponseCommand processVesionCommand(final VersionCommand request, final SessionContext ctx) {
        return this.next.processVesionCommand(request, ctx);
    }


    @Override
    public ResponseCommand processStatCommand(final StatsCommand request, final SessionContext ctx) {
        return this.next.processStatCommand(request, ctx);
    }


    @Override
    public void removeTransaction(final XATransactionId xid) {
        this.next.removeTransaction(xid);
    }


    @Override
    public Transaction getTransaction(final SessionContext context, final TransactionId xid)
            throws MetamorphosisException, XAException {
        return this.next.getTransaction(context, xid);
    }


    @Override
    public void forgetTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        this.next.forgetTransaction(context, xid);
    }


    @Override
    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        this.next.rollbackTransaction(context, xid);
    }


    @Override
    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase)
            throws Exception {
        this.next.commitTransaction(context, xid, onePhase);
    }


    @Override
    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        return this.next.prepareTransaction(context, xid);
    }


    @Override
    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds)
            throws Exception {
        this.next.beginTransaction(context, xid, seconds);
    }


    @Override
    public TransactionId[] getPreparedTransactions(final SessionContext context, final String uniqueQualifier)
            throws Exception {
        return this.next.getPreparedTransactions(context, uniqueQualifier);
    }


    @Override
    public ResponseCommand processGetCommand(final GetCommand request, final SessionContext ctx, final boolean zeroCopy) {
        return this.next.processGetCommand(request, ctx, zeroCopy);
    }

}