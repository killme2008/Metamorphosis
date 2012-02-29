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
package com.taobao.metamorphosis.gregor.slave;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.PutProcessor;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.network.SessionContextHolder;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * syncÃüÁîµÄÍøÂç´¦ÀíÆ÷
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class SyncProcessor implements RequestProcessor<SyncCommand> {
    static final Log log = LogFactory.getLog(PutProcessor.class);

    private final SyncCommandProcessor processor;
    private final OrderedThreadPoolExecutor executor;


    public SyncProcessor(final SyncCommandProcessor processor, final OrderedThreadPoolExecutor executor) {
        super();
        this.processor = processor;
        this.executor = executor;
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }


    @Override
    public void handleRequest(final SyncCommand request, final Connection conn) {
        this.executor.execute(new IoEvent() {

            @Override
            public void run() {
                final TransactionId xid = request.getTransactionId();
                final SessionContext context = SessionContextHolder.getOrCreateSessionContext(conn, xid);
                try {
                    SyncProcessor.this.processor.processSyncCommand(request, context, new PutCallback() {
                        @Override
                        public void putComplete(final ResponseCommand resp) {
                            RemotingUtils.response(context.getConnection(), resp);
                        }
                    });
                }
                catch (final Exception e) {
                    RemotingUtils.response(context.getConnection(), new BooleanCommand(request.getOpaque(),
                        HttpStatus.InternalServerError, e.getMessage()));
                }

            }


            @Override
            public IoCatalog getIoCatalog() {
                return new IoCatalog(conn, request.getTopic() + "-" + request.getPartition());
            }
        });

    }
}