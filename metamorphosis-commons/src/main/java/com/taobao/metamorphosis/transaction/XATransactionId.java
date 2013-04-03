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

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

import javax.transaction.xa.Xid;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.utils.HexSupport;
import com.taobao.metamorphosis.utils.PatternUtils;


/**
 * XAÊÂÎñid
 * 
 * @author boyan
 * 
 */
public class XATransactionId extends TransactionId implements Xid, Comparable<XATransactionId>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 7130168769221529970L;
    private int formatId;
    private byte[] branchQualifier;
    private byte[] globalTransactionId;

    private transient int hash;
    private transient String transactionKey;
    private String uniqueQualifier;


    public String getUniqueQualifier() {
        return this.uniqueQualifier;
    }


    public void setUniqueQualifier(String uniqueQualifier) {
        this.uniqueQualifier = uniqueQualifier;
    }


    @Override
    public boolean isNull() {
        return false;
    }


    public XATransactionId() {
    }


    /**
     * Just for test
     * 
     * @param formatId
     * @param branchQualifier
     * @param globalTransactionId
     */
    public XATransactionId(final int formatId, final byte[] branchQualifier, final byte[] globalTransactionId,
            final String uniqueQualifier) {
        super();
        this.uniqueQualifier = uniqueQualifier;
        this.formatId = formatId;
        this.branchQualifier = branchQualifier;
        this.globalTransactionId = globalTransactionId;
    }

    static final Pattern pattern = Pattern.compile(":");


    public XATransactionId(final String key) {
        final String[] tmps = PatternUtils.split(pattern, key);
        if (tmps.length != 5) {
            throw new IllegalArgumentException("Illegal transaction key" + key);
        }
        assert tmps[0].equals("XID");
        this.formatId = Integer.parseInt(tmps[1]);
        this.globalTransactionId = HexSupport.toBytesFromHex(tmps[2]);
        this.branchQualifier = HexSupport.toBytesFromHex(tmps[3]);
        this.uniqueQualifier = tmps[4];
    }


    public XATransactionId(final Xid xid, final String uniqueQualifier) {
        this.formatId = xid.getFormatId();
        this.globalTransactionId = xid.getGlobalTransactionId();
        this.branchQualifier = xid.getBranchQualifier();
        this.uniqueQualifier = uniqueQualifier;
        if (StringUtils.isBlank(uniqueQualifier)) {
            throw new IllegalArgumentException("Blank uniqueQualifier");
        }
    }


    @Override
    public synchronized String getTransactionKey() {
        if (this.transactionKey == null) {
            this.transactionKey =
                    "XID:" + this.formatId + ":" + HexSupport.toHexFromBytes(this.globalTransactionId) + ":"
                            + HexSupport.toHexFromBytes(this.branchQualifier) + ":" + this.uniqueQualifier;
        }
        return this.transactionKey;
    }


    @Override
    public String toString() {
        return this.getTransactionKey();
    }


    @Override
    public boolean isXATransaction() {
        return true;
    }


    @Override
    public boolean isLocalTransaction() {
        return false;
    }


    /**
     * @openwire:property version=1
     */
    @Override
    public int getFormatId() {
        return this.formatId;
    }


    /**
     * @openwire:property version=1
     */
    @Override
    public byte[] getGlobalTransactionId() {
        return this.globalTransactionId;
    }


    /**
     * @openwire:property version=1
     */
    @Override
    public byte[] getBranchQualifier() {
        return this.branchQualifier;
    }


    public void setBranchQualifier(final byte[] branchQualifier) {
        this.branchQualifier = branchQualifier;
        this.hash = 0;
    }


    public void setFormatId(final int formatId) {
        this.formatId = formatId;
        this.hash = 0;
    }


    public void setGlobalTransactionId(final byte[] globalTransactionId) {
        this.globalTransactionId = globalTransactionId;
        this.hash = 0;
    }


    @Override
    public int hashCode() {
        if (this.hash == 0) {
            this.hash = this.formatId;
            this.hash = hash(this.globalTransactionId, this.hash);
            this.hash = hash(this.branchQualifier, this.hash);
            final int prime = 31;
            this.hash = prime * this.hash + this.uniqueQualifier.hashCode();
            if (this.hash == 0) {
                this.hash = 0xaceace;
            }
        }
        return this.hash;
    }


    private static int hash(final byte[] bytes, int hash) {
        final int size = bytes.length;
        for (int i = 0; i < size; i++) {
            hash ^= bytes[i] << i % 4 * 8;
        }
        return hash;
    }


    @Override
    public boolean equals(final Object o) {
        if (o == null || !(o instanceof XATransactionId)) {
            return false;
        }
        final XATransactionId xid = (XATransactionId) o;
        return xid.formatId == this.formatId && Arrays.equals(xid.globalTransactionId, this.globalTransactionId)
                && Arrays.equals(xid.branchQualifier, this.branchQualifier)
                && this.uniqueQualifier.equals(xid.uniqueQualifier);
    }


    @Override
    public int compareTo(final XATransactionId o) {
        if (o == null || o.getClass() != XATransactionId.class) {
            return -1;
        }
        final XATransactionId xid = o;
        return this.getTransactionKey().compareTo(xid.getTransactionKey());
    }

}