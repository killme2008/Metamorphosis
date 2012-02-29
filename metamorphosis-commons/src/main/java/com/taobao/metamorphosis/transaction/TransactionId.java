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


/**
 * 事务id包装类
 * 
 * @author boyan
 * 
 */
public abstract class TransactionId implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 2157471469471230263L;
    public static final NullTransactionId Null = new NullTransactionId();


    public abstract boolean isXATransaction();


    public abstract boolean isLocalTransaction();


    public abstract String getTransactionKey();


    public abstract boolean isNull();


    public static TransactionId valueOf(final String key) {
        if (key.equals("null")) {
            return TransactionId.Null;
        }
        else if (key.startsWith("XID:")) {
            return new XATransactionId(key);
        }
        else if (key.startsWith("TX:")) {
            return new LocalTransactionId(key);

        }
        else {
            throw new IllegalArgumentException("Illegal transaction key:" + key);
        }
    }

}