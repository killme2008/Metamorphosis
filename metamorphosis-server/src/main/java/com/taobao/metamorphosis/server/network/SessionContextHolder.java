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

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * SessionContext管理类
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public class SessionContextHolder {
    private SessionContextHolder() {

    }

    public static final String GLOBAL_SESSION_KEY = "SessionContextGlobalKey" + System.currentTimeMillis();


    public static SessionContext getOrCreateSessionContext(final Connection conn, final TransactionId xid) {
        SessionContext context = null;
        if (xid != null && xid.isLocalTransaction()) {
            // 本地事务带有session id，因此用sessionId做key存储
            final LocalTransactionId id = (LocalTransactionId) xid;
            context = (SessionContext) conn.getAttribute(id.getSessionId());
            if (context == null) {
                context = new SessionContextImpl(id.getSessionId(), conn);
                final SessionContext old = (SessionContext) conn.setAttributeIfAbsent(id.getSessionId(), context);
                if (old != null) {
                    context = old;
                }
            }
        }
        else {
            // XA事务没有session id，使用公共key，减少重复new
            context = (SessionContext) conn.getAttribute(GLOBAL_SESSION_KEY);
            if (context == null) {
                context = new SessionContextImpl(null, conn);
                final SessionContext old = (SessionContext) conn.setAttributeIfAbsent(GLOBAL_SESSION_KEY, context);
                if (old != null) {
                    context = old;
                }
            }

        }
        return context;
    }
}