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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;


public class TransactionIdUnitTest {
    private final Random rand = new Random();


    private byte[] randomBytes() {
        final byte[] bytes = new byte[this.rand.nextInt(100)];
        this.rand.nextBytes(bytes);
        return bytes;
    }


    @Test
    public void testValueOf() {
        final byte[] branchQualifier = this.randomBytes();
        final byte[] globalTransactionId = this.randomBytes();
        final XATransactionId xid = new XATransactionId(100, branchQualifier, globalTransactionId, "unique-qualifier");
        final LocalTransactionId id = new LocalTransactionId("sessionId", -99);

        final String key1 = xid.getTransactionKey();
        final String key2 = id.getTransactionKey();

        assertEquals(xid, TransactionId.valueOf(key1));
        assertEquals(id, TransactionId.valueOf(key2));
    }


    @Test
    public void testXATransactionId() {
        final byte[] branchQualifier = this.randomBytes();
        final byte[] globalTransactionId = this.randomBytes();
        final XATransactionId id = new XATransactionId(100, branchQualifier, globalTransactionId, "unique-qualifier");
        assertArrayEquals(branchQualifier, id.getBranchQualifier());
        assertArrayEquals(globalTransactionId, id.getGlobalTransactionId());
        assertEquals(100, id.getFormatId());
        assertEquals("unique-qualifier", id.getUniqueQualifier());

        final String key = id.getTransactionKey();
        assertNotNull(key);
        final XATransactionId newId = new XATransactionId(key);
        assertNotSame(id, newId);
        assertEquals(id, newId);
        assertEquals(0, id.compareTo(newId));

        assertTrue(id.isXATransaction());
        assertFalse(id.isLocalTransaction());
    }


    @Test
    public void testLocalTransactionId() {
        final LocalTransactionId id = new LocalTransactionId("sessionId", -99);
        assertFalse(id.isXATransaction());
        assertTrue(id.isLocalTransaction());
        assertEquals("sessionId", id.getSessionId());
        assertEquals(-99, id.getValue());
        final String s = id.getTransactionKey();
        assertNotNull(s);
        final LocalTransactionId newId = new LocalTransactionId(s);
        assertEquals(id, newId);
        assertEquals(0, id.compareTo(newId));
    }
}