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
package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;


public class MessageIteratorUnitTest {
    private MessageIterator it;


    @Test
    public void testHasNext_NullData() {
        this.it = new MessageIterator("test", null);
        assertFalse(this.it.hasNext());

    }


    @Test
    public void testHasNext_EmptyData() {
        this.it = new MessageIterator("test", new byte[0]);
        assertFalse(this.it.hasNext());

    }


    @Test
    public void testHasNext_End() {
        this.it = new MessageIterator("test", new byte[16]);
        this.it.setOffset(16);
        assertFalse(this.it.hasNext());
    }


    @Test
    public void testHasNext_Over() {
        this.it = new MessageIterator("test", new byte[16]);
        this.it.setOffset(17);
        assertFalse(this.it.hasNext());
    }


    @Test
    public void testIteratorAsKey() {
        this.it = new MessageIterator("test", new byte[16]);
        final Map<MessageIterator, Integer> map = new HashMap<MessageIterator, Integer>();
        assertNull(map.get(this.it));
        map.put(this.it, 100);
        assertEquals((Integer) 100, map.get(this.it));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void tsetRemove() {
        this.it = new MessageIterator("test", new byte[16]);
        this.it.remove();
    }


    @Test
    public void testHasNext_HeaderNotComplete() {
        this.it = new MessageIterator("test", new byte[16]);
        assertFalse(this.it.hasNext());
    }


    @Test
    public void testHasNext_NotEnoughPayload() {
        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN);
        buf.putInt(20);// msg length

        this.it = new MessageIterator("test", buf.array());
        assertFalse(this.it.hasNext());
    }


    @Test
    public void testHasNext_true() {
        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN + 4);
        buf.putInt(4);// msg length
        buf.position(20);
        buf.putInt(99); // payload
        this.it = new MessageIterator("test", buf.array());
        assertTrue(this.it.hasNext());
    }


    @Test(expected = InvalidMessageException.class)
    public void testNext_InvalidMessage() throws Exception {
        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN + 5);
        buf.putInt(5);// msg length
        buf.putInt(CheckSum.crc32("hello".getBytes())); // checksum

        buf.position(MessageUtils.HEADER_LEN);
        buf.put("world".getBytes());
        this.it = new MessageIterator("test", buf.array());
        assertTrue(this.it.hasNext());
        this.it.next();

    }


    @Test
    public void testNext_NoAttribute() throws Exception {
        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN + 5);
        buf.putInt(5);// msg length
        buf.putInt(CheckSum.crc32("hello".getBytes())); // checksum
        buf.putLong(9999); // id
        buf.putInt(0); // flag

        buf.position(MessageUtils.HEADER_LEN);
        buf.put("hello".getBytes());
        this.it = new MessageIterator("test", buf.array());
        assertTrue(this.it.hasNext());
        final Message msg = this.it.next();
        assertNotNull(msg);
        assertEquals(9999L, msg.getId());
        assertEquals("test", msg.getTopic());
        assertFalse(msg.hasAttribute());
        assertEquals(0, MessageAccessor.getFlag(msg));
        assertEquals("hello", new String(msg.getData()));

    }


    @Test
    public void testNext_HasAttribute() throws Exception {
        final ByteBuffer dataBuf = ByteBuffer.allocate(18);

        dataBuf.putInt(9);
        dataBuf.put("attribute".getBytes());
        dataBuf.put("hello".getBytes());
        dataBuf.flip();

        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN + 4 + 9 + 5);
        buf.putInt(18);// msg length
        buf.putInt(CheckSum.crc32(dataBuf.array())); // checksum
        buf.putLong(9999); // id
        buf.putInt(1); // flag
        buf.position(MessageUtils.HEADER_LEN);
        buf.put(dataBuf);

        this.it = new MessageIterator("test", buf.array());
        assertTrue(this.it.hasNext());
        final Message msg = this.it.next();
        assertNotNull(msg);
        assertEquals(9999L, msg.getId());
        assertEquals("test", msg.getTopic());
        assertTrue(msg.hasAttribute());
        assertEquals("attribute", msg.getAttribute());
        assertEquals(1, MessageAccessor.getFlag(msg));
        assertEquals("hello", new String(msg.getData()));
        assertFalse(this.it.hasNext());
        assertEquals(38, this.it.getOffset());

    }
}