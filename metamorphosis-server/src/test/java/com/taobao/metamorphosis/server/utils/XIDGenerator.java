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
package com.taobao.metamorphosis.server.utils;

import java.util.Random;

import com.taobao.metamorphosis.transaction.XATransactionId;


/**
 * 产生xid的工具类，仅用于测试
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-24
 * 
 */
public class XIDGenerator {
    public static final String UNIQUE_QUALIFIER = "unique-qualifier";
    private final static Random rand = new Random();


    private static byte[] randomBytes() {
        final byte[] bytes = new byte[rand.nextInt(100)];
        rand.nextBytes(bytes);
        return bytes;
    }


    public static XATransactionId createXID(final int formatId, String uniqueQualifier) {
        final byte[] branchQualifier = randomBytes();
        final byte[] globalTransactionId = randomBytes();
        final XATransactionId xid =
                new XATransactionId(formatId, branchQualifier, globalTransactionId, uniqueQualifier);
        return xid;
    }


    public static XATransactionId createXID(final int formatId) {
        return createXID(formatId, UNIQUE_QUALIFIER);
    }
}