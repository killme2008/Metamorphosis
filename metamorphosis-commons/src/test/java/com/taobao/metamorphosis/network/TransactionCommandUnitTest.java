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
package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.TransactionInfo;
import com.taobao.metamorphosis.transaction.TransactionInfo.TransactionType;


public class TransactionCommandUnitTest {

    @Test
    public void testEncode() {
        final TransactionId id = new LocalTransactionId("sessionId", 99);
        final TransactionInfo info = new TransactionInfo(id, "sessionId", TransactionType.COMMIT_ONE_PHASE, null);

        final TransactionCommand cmd = new TransactionCommand(info, 100);
        final IoBuffer buf = cmd.encode();
        assertEquals("transaction TX:sessionId:99 sessionId COMMIT_ONE_PHASE 100\r\n", new String(buf.array()));
    }


    @Test
    public void testEncodeWithTimeout() {
        final TransactionId id = new LocalTransactionId("sessionId", 99);
        final TransactionInfo info = new TransactionInfo(id, "sessionId", TransactionType.BEGIN, null, 3);

        final TransactionCommand cmd = new TransactionCommand(info, 100);
        final IoBuffer buf = cmd.encode();
        assertEquals("transaction TX:sessionId:99 sessionId BEGIN 3 100\r\n", new String(buf.array()));
    }


    @Test
    public void testEncodeWithUniqueQualifier() {
        final TransactionId id = new LocalTransactionId("sessionId", 99);
        final TransactionInfo info = new TransactionInfo(id, "sessionId", TransactionType.BEGIN, "unique-qualifier");

        final TransactionCommand cmd = new TransactionCommand(info, 100);
        final IoBuffer buf = cmd.encode();
        assertEquals("transaction TX:sessionId:99 sessionId BEGIN unique-qualifier 100\r\n", new String(buf.array()));
    }


    @Test
    public void testEncodeWithUniqueQualifierAndTimeout() {
        final TransactionId id = new LocalTransactionId("sessionId", 99);
        final TransactionInfo info = new TransactionInfo(id, "sessionId", TransactionType.BEGIN, "unique-qualifier", 3);

        final TransactionCommand cmd = new TransactionCommand(info, 100);
        final IoBuffer buf = cmd.encode();
        assertEquals("transaction TX:sessionId:99 sessionId BEGIN 3 unique-qualifier 100\r\n", new String(buf.array()));
    }
}