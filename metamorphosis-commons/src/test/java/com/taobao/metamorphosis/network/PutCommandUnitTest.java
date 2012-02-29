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


public class PutCommandUnitTest {

    @Test
    public void testEncode_HasTransactionId() {
        final TransactionId id = new LocalTransactionId("test", 1);
        final PutCommand putCommand = new PutCommand("test", 1, "hello".getBytes(), id, 0, 0);
        final IoBuffer buf = putCommand.encode();
        assertEquals(0, buf.position());
        assertEquals("put test 1 5 0 TX:test:1 0\r\nhello", new String(buf.array()));
    }


    @Test
    public void testEncode_NoTransactionId() {
        final PutCommand putCommand = new PutCommand("test", 1, "hello".getBytes(), null, 0, 0);
        final IoBuffer buf = putCommand.encode();
        assertEquals(0, buf.position());
        assertEquals("put test 1 5 0 0\r\nhello", new String(buf.array()));
    }
}