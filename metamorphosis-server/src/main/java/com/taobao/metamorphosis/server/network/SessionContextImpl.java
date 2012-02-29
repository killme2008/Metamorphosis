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

import java.util.concurrent.ConcurrentHashMap;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 会话上下文
 * 
 * @author boyan
 * 
 */
public class SessionContextImpl implements SessionContext {
    private final ConcurrentHashMap<TransactionId, Transaction> transactions =
            new ConcurrentHashMap<TransactionId, Transaction>();
    private String sessionId;
    private Connection connection;
    private boolean inRecoverMode;


    public SessionContextImpl(final String sessionId, final Connection connection) {
        super();
        this.sessionId = sessionId;
        this.connection = connection;
    }


    @Override
    public boolean isInRecoverMode() {
        return this.inRecoverMode;
    }


    public void setInRecoverMode(final boolean inRecoverMode) {
        this.inRecoverMode = inRecoverMode;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.server.network.SessionContext#getTransactions()
     */
    @Override
    public ConcurrentHashMap<TransactionId, Transaction> getTransactions() {
        return this.transactions;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.server.network.SessionContext#getSessionId()
     */
    @Override
    public String getSessionId() {
        return this.sessionId;
    }


    public void setSessionId(final String sessionId) {
        this.sessionId = sessionId;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.server.network.SessionContext#getConnection()
     */
    @Override
    public Connection getConnection() {
        return this.connection;
    }


    public void setConnection(final Connection connection) {
        this.connection = connection;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.connection == null ? 0 : this.connection.hashCode());
        result = prime * result + (this.sessionId == null ? 0 : this.sessionId.hashCode());
        result = prime * result + (this.transactions == null ? 0 : this.transactions.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final SessionContextImpl other = (SessionContextImpl) obj;
        if (this.connection == null) {
            if (other.connection != null) {
                return false;
            }
        }
        else if (!this.connection.equals(other.connection)) {
            return false;
        }
        if (this.sessionId == null) {
            if (other.sessionId != null) {
                return false;
            }
        }
        else if (!this.sessionId.equals(other.sessionId)) {
            return false;
        }
        if (this.transactions == null) {
            if (other.transactions != null) {
                return false;
            }
        }
        else if (!this.transactions.equals(other.transactions)) {
            return false;
        }
        return true;
    }

}