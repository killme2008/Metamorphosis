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
import java.io.Serializable;

import javax.transaction.xa.XAException;

import org.apache.commons.logging.Log;

import com.taobao.gecko.service.timer.Timeout;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 事务基类
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-23
 * 
 */
public abstract class Transaction implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -5907949950359568586L;
    public static final byte START_STATE = 0; // can go to: 1,2,3
    public static final byte IN_USE_STATE = 1; // can go to: 2,3
    public static final byte PREPARED_STATE = 2; // can go to: 3
    public static final byte FINISHED_STATE = 3;
    public static final byte HEURISTIC_COMMIT_STATE = 4;
    public static final byte HEURISTIC_ROLLBACK_STATE = 5;
    public static final byte HEURISTIC_COMPLETE_STATE = 6;

    private volatile transient Timeout timeoutRef;

    private volatile byte state = START_STATE;


    public byte getState() {
        return this.state;
    }


    public Timeout getTimeoutRef() {
        return this.timeoutRef;
    }


    public void setTimeoutRef(final Timeout timeoutRef) {
        this.timeoutRef = timeoutRef;
    }


    public void setState(final byte state) {
        if (state == FINISHED_STATE) {
            // 终止超时检测
            if (this.timeoutRef != null) {
                this.timeoutRef.cancel();
            }
        }
        this.state = state;
    }


    /**
     * 设置事务正在被使用中
     */
    public void setTransactionInUse() {
        if (this.state == START_STATE) {
            this.state = IN_USE_STATE;
        }
    }


    protected void cancelTimeout() {
        if (this.timeoutRef != null) {
            this.timeoutRef.cancel();
        }
    }


    public void prePrepare() throws Exception {
        // Is it ok to call prepare now given the state of the
        // transaction?
        switch (this.state) {
        case START_STATE:
        case IN_USE_STATE:
            break;
        default:
            final XAException xae = new XAException("Prepare cannot be called now.");
            xae.errorCode = XAException.XAER_PROTO;
            throw xae;
        }
    }


    public abstract void commit(boolean onePhase) throws XAException, IOException;


    public abstract void rollback() throws XAException, IOException;


    public abstract int prepare() throws XAException, IOException;


    public abstract TransactionId getTransactionId();


    public abstract Log getLog();


    public boolean isPrepared() {
        return this.getState() == PREPARED_STATE;
    }

}