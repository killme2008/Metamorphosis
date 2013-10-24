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
package com.taobao.metamorphosis.transaction;

import javax.transaction.xa.XAException;


/**
 * 事务信息
 * 
 * @author boyan
 * 
 */
public class TransactionInfo {

    private String uniqueQualifier;
    private TransactionId transactionId;
    private final String sessionId;
    private final TransactionType type;
    private int timeout = 0;


    public void setUniqueQualifier(String uniqueQualifier) {
        this.uniqueQualifier = uniqueQualifier;
    }


    public String getUniqueQualifier() {
        return this.uniqueQualifier;
    }

    /**
     * 事务状态
     * 
     * @author boyan
     * 
     */
    public static enum TransactionType {
        BEGIN,
        PREPARE,
        COMMIT_ONE_PHASE,
        COMMIT_TWO_PHASE,
        ROLLBACK,
        RECOVER,
        FORGET,
        END
    }


    public int getTimeout() {
        return this.timeout;
    }


    /**
     * 设置事务超时
     * 
     * @param timeout
     * @throws XAException
     */
    public void setTimeout(final int timeout) throws XAException {
        if (timeout < 0) {
            throw new XAException(XAException.XAER_INVAL);
        }
        this.timeout = timeout;
    }


    public TransactionInfo(final TransactionId transactionId, final String sessionId, final TransactionType type) {
        this(transactionId, sessionId, type, null, 0);
    }


    public TransactionInfo(final TransactionId transactionId, final String sessionId, final TransactionType type,
            final String uniqueQualifier) {
        this(transactionId, sessionId, type, uniqueQualifier, 0);
    }


    public TransactionInfo(final TransactionId transactionId, final String sessionId, final TransactionType type,
            final String uniqueQualifier, final int timeout) {
        super();
        this.setTransactionId(transactionId);
        this.sessionId = sessionId;
        this.type = type;
        this.uniqueQualifier = uniqueQualifier;
        this.timeout = timeout;
    }


    private void setTransactionId(final TransactionId transactionId) {
        if (transactionId == null) {
            this.transactionId = TransactionId.Null;
        }
        else {
            this.transactionId = transactionId;
        }
    }


    public TransactionId getTransactionId() {
        return this.transactionId;
    }


    public String getSessionId() {
        return this.sessionId;
    }


    public TransactionType getType() {
        return this.type;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.sessionId == null ? 0 : this.sessionId.hashCode());
        result = prime * result + this.timeout;
        result = prime * result + (this.transactionId == null ? 0 : this.transactionId.hashCode());
        result = prime * result + (this.type == null ? 0 : this.type.hashCode());
        result = prime * result + (this.uniqueQualifier == null ? 0 : this.uniqueQualifier.hashCode());
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
        final TransactionInfo other = (TransactionInfo) obj;
        if (this.sessionId == null) {
            if (other.sessionId != null) {
                return false;
            }
        }
        else if (!this.sessionId.equals(other.sessionId)) {
            return false;
        }
        if (this.timeout != other.timeout) {
            return false;
        }
        if (this.transactionId == null) {
            if (other.transactionId != null) {
                return false;
            }
        }
        else if (!this.transactionId.equals(other.transactionId)) {
            return false;
        }
        if (this.uniqueQualifier == null) {
            if (other.uniqueQualifier != null) {
                return false;
            }
        }
        else if (!this.uniqueQualifier.equals(other.uniqueQualifier)) {
            return false;
        }
        if (this.type != other.type) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "TransactionInfo [transactionId=" + this.transactionId + ", sessionId=" + this.sessionId + ", type="
                + this.type + ", uniqueQualifier=" + this.uniqueQualifier + ", timeout=" + this.timeout + "]";
    }

}