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
package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import javax.transaction.xa.XAException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.network.TransactionCommand;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 事务命令处理器
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-17
 * 
 */
public class TransactionProcessor implements RequestProcessor<TransactionCommand> {

    private final CommandProcessor processor;

    private final ThreadPoolExecutor executor;


    public TransactionProcessor(final CommandProcessor processor, final ThreadPoolExecutor executor) {
        super();
        this.processor = processor;
        this.executor = executor;
    }


    @Override
    public void handleRequest(final TransactionCommand request, final Connection conn) {

        final TransactionId xid = request.getTransactionInfo().getTransactionId();
        final SessionContext context = SessionContextHolder.getOrCreateSessionContext(conn, xid);

        if (log.isDebugEnabled()) {
            log.debug(request);
        }
        try {
            switch (request.getTransactionInfo().getType()) {
            case BEGIN:
                this.processor.beginTransaction(context, xid, request.getTransactionInfo().getTimeout());
                this.responseOK(request, conn);
                break;
            case END:
                // ignore;
                break;
            case PREPARE:
                final int rt = this.processor.prepareTransaction(context, xid);
                // prepare结果需返回
                RemotingUtils.response(conn,
                    new BooleanCommand(HttpStatus.Success, String.valueOf(rt), request.getOpaque()));
                break;
                // 提交和,forget和rollback的时候是同步调用，因此需要应答
            case COMMIT_ONE_PHASE:
                this.processor.commitTransaction(context, xid, true);
                this.responseOK(request, conn);
                break;
            case COMMIT_TWO_PHASE:
                this.processor.commitTransaction(context, xid, false);
                this.responseOK(request, conn);
                break;
            case FORGET:
                this.processor.forgetTransaction(context, xid);
                this.responseOK(request, conn);
                break;
            case ROLLBACK:
                this.processor.rollbackTransaction(context, xid);
                this.responseOK(request, conn);
                break;
            case RECOVER:
                final TransactionId[] xids =
                        this.processor.getPreparedTransactions(context, request.getTransactionInfo()
                            .getUniqueQualifier());
                final StringBuilder sb = new StringBuilder();
                boolean wasFirst = true;
                for (final TransactionId id : xids) {
                    if (wasFirst) {
                        sb.append(id.getTransactionKey());
                        wasFirst = false;
                    }
                    else {
                        sb.append("\r\n").append(id.getTransactionKey());
                    }
                }
                RemotingUtils
                .response(conn, new BooleanCommand(HttpStatus.Success, sb.toString(), request.getOpaque()));
                break;
            default:
                RemotingUtils.response(conn, new BooleanCommand(HttpStatus.InternalServerError, "Unknow transaction command type:" + request.getTransactionInfo().getType(),
                    request.getOpaque()));

            }
        }
        catch (final XAException e) {
            log.error("Processing transaction command failed", e);
            // xa异常特殊处理，让客户端可以直接抛出
            this.responseXAE(request, conn, e);
        }
        catch (final Exception e) {
            log.error("Processing transaction command failed", e);
            if (e.getCause() instanceof XAException) {
                this.responseXAE(request, conn, (XAException) e.getCause());
            }
            else {
                this.responseError(request, conn, e);
            }
        }
    }


    private void responseError(final TransactionCommand request, final Connection conn, final Exception e) {
        RemotingUtils.response(conn,
            new BooleanCommand(HttpStatus.InternalServerError, e.getMessage(), request.getOpaque()));
    }


    private void responseXAE(final TransactionCommand request, final Connection conn, final XAException e) {
        RemotingUtils.response(conn, new BooleanCommand(HttpStatus.InternalServerError, "XAException:code=" + e.errorCode + ",msg=" + e.getMessage(),
            request.getOpaque()));
    }

    static final Log log = LogFactory.getLog(TransactionProcessor.class);


    private void responseOK(final TransactionCommand request, final Connection conn) {
        RemotingUtils.response(conn, new BooleanCommand(HttpStatus.Success, null, request.getOpaque()));
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return this.executor;
    }

}