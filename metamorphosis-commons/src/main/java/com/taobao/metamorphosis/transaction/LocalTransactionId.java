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

import java.util.regex.Pattern;


/**
 * 本地事务id
 * 
 * @author boyan
 * 
 */
public class LocalTransactionId extends TransactionId implements Comparable<LocalTransactionId> {

    /**
     * 
     */
    private static final long serialVersionUID = -8264253260623180909L;
    protected String sessionId;
    protected long value;

    private transient String transactionKey;
    private transient int hashCode;


    @Override
    public boolean isNull() {
        return false;
    }


    public LocalTransactionId() {
    }

    static final Pattern pattern = Pattern.compile(":");


    public LocalTransactionId(final String key) {
        final String[] tmps = pattern.split(key);
        if (tmps.length != 3) {
            throw new IllegalArgumentException("Illegal transaction key:" + key);
        }
        assert tmps[0].equals("TX");
        this.sessionId = tmps[1];
        this.value = Integer.parseInt(tmps[2]);

    }


    public LocalTransactionId(final String sessionId, final long transactionId) {
        this.sessionId = sessionId;
        this.value = transactionId;
    }


    @Override
    public boolean isXATransaction() {
        return false;
    }


    @Override
    public boolean isLocalTransaction() {
        return true;
    }


    @Override
    public String getTransactionKey() {
        if (this.transactionKey == null) {
            this.transactionKey = "TX:" + this.sessionId + ":" + this.value;
        }
        return this.transactionKey;
    }


    @Override
    public String toString() {
        return this.getTransactionKey();
    }


    @Override
    public int hashCode() {
        if (this.hashCode == 0) {
            this.hashCode = this.sessionId.hashCode() ^ (int) this.value;
        }
        return this.hashCode;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof LocalTransactionId)) {
            return false;
        }
        final LocalTransactionId tx = (LocalTransactionId) o;
        return this.value == tx.value && this.sessionId.equals(tx.sessionId);
    }


    /**
     * @param o
     * @return
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(final LocalTransactionId o) {
        int result = this.sessionId.compareTo(o.sessionId);
        if (result == 0) {
            result = (int) (this.value - o.value);
        }
        return result;
    }


    public long getValue() {
        return this.value;
    }


    public void setValue(final long transactionId) {
        this.value = transactionId;
    }


    public String getSessionId() {
        return this.sessionId;
    }


    public void setConnectionId(final String sessionId) {
        this.sessionId = sessionId;
    }

}