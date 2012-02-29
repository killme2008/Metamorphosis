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
package com.taobao.metamorphosis.server.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.network.SessionContextImpl;
import com.taobao.metamorphosis.utils.MessageUtils;


public class FileMessageSetUnitTest {
    private FileMessageSet fileMessageSet;
    private File file;


    @Before
    public void setUp() throws IOException {
        this.file = new File("FileMessageSetUnitTest.test");
        if (!this.file.exists()) {
            this.file.createNewFile();
        }

        final FileChannel channel = new RandomAccessFile(this.file, "rw").getChannel();
        channel.truncate(0);
        this.fileMessageSet = new FileMessageSet(channel);
    }


    @After
    public void tearDown() throws IOException {
        if (this.fileMessageSet != null) {
            this.fileMessageSet.close();
        }
        if (this.file != null && this.file.exists()) {
            this.file.delete();
        }

    }


    @Test
    public void testMakeHead() throws Exception {
        final IoBuffer head = this.fileMessageSet.makeHead(-1999, 100);
        assertEquals(0, head.position());
        assertTrue(head.hasRemaining());
        assertEquals("value 100 -1999\r\n", new String(head.array()));
    }


    @Test
    public void testSlice() throws Exception {
        final long limit = 100;
        this.fileMessageSet.channel().position(limit);
        final ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) 1);
        buf.flip();
        this.fileMessageSet.append(buf);
        this.fileMessageSet.setSizeInBytes(limit);
        this.fileMessageSet.flush();
        assertEquals(limit, this.fileMessageSet.highWaterMark());

        FileMessageSet subSet = (FileMessageSet) this.fileMessageSet.slice(1, 10);
        assertNotNull(subSet);
        assertEquals(9, subSet.getSizeInBytes());
        assertEquals(1, subSet.getOffset());

        subSet = (FileMessageSet) this.fileMessageSet.slice(10, 210);
        assertNotNull(subSet);
        assertEquals(limit + 1 - 10, subSet.getSizeInBytes());
        assertEquals(10, subSet.getOffset());
    }


    @Test
    public void testAppendAppendCloseRecover() throws Exception {
        PutCommand message = new PutCommand("test", 0, "hello".getBytes(), null, 0, 0);
        final ByteBuffer buf1 = MessageUtils.makeMessageBuffer(1, message);
        assertEquals(0, this.fileMessageSet.append(buf1));

        message = new PutCommand("test", 0, "world".getBytes(), null, 0, 0);
        final ByteBuffer buf2 = MessageUtils.makeMessageBuffer(2, message);
        assertEquals(buf1.capacity(), this.fileMessageSet.append(buf2));

        this.fileMessageSet.flush();
        this.fileMessageSet.close();

        final FileChannel channel = new RandomAccessFile(this.file, "rw").getChannel();
        this.fileMessageSet = new FileMessageSet(channel);
        assertEquals(2 * buf2.capacity(), this.fileMessageSet.getSizeInBytes());
        assertEquals(2 * buf2.capacity(), this.fileMessageSet.channel().position());
    }


    @Test
    public void testAppendAppendCloseRecoverTruncate() throws Exception {
        PutCommand message = new PutCommand("test", 0, "hello".getBytes(), null, 0, 0);
        ByteBuffer buf = MessageUtils.makeMessageBuffer(1, message);
        this.fileMessageSet.append(buf);

        message = new PutCommand("test", 0, "world".getBytes(), null, 0, 0);
        buf = MessageUtils.makeMessageBuffer(2, message);
        this.fileMessageSet.append(buf);
        this.fileMessageSet.append(ByteBuffer.wrap("temp".getBytes()));

        this.fileMessageSet.flush();
        this.fileMessageSet.close();

        final FileChannel channel = new RandomAccessFile(this.file, "rw").getChannel();
        this.fileMessageSet = new FileMessageSet(channel);
        assertEquals(2 * buf.capacity(), this.fileMessageSet.getSizeInBytes());
        assertEquals(2 * buf.capacity(), this.fileMessageSet.channel().position());
    }


    @Test
    public void testAppendAppendFlushSliceWrite() throws IOException {
        final String str = "hello world";
        final ByteBuffer buf = ByteBuffer.wrap(str.getBytes());
        assertEquals(0, this.fileMessageSet.append(buf));
        buf.rewind();
        assertEquals(buf.capacity(), this.fileMessageSet.append(buf));
        assertEquals(0L, this.fileMessageSet.highWaterMark());
        this.fileMessageSet.flush();
        assertEquals(2L * str.length(), this.fileMessageSet.highWaterMark());
        final FileMessageSet subSet = (FileMessageSet) this.fileMessageSet.slice(0, 100);

        assertEquals(2L * str.length(), subSet.highWaterMark());
        final Connection conn = EasyMock.createMock(Connection.class);
        EasyMock.expect(conn.getRemoteSocketAddress()).andReturn(new InetSocketAddress(8181)).anyTimes();

        final int opaque = 99;
        final IoBuffer head = IoBuffer.wrap(("value " + 2 * str.length() + " " + opaque + "\r\n").getBytes());
        conn.transferFrom(head, null, this.fileMessageSet.channel(), 0, 2 * str.length());
        EasyMock.expectLastCall();
        EasyMock.replay(conn);

        subSet.write(new GetCommand("test", "boyan-test", -1, 0, 1024 * 1024, opaque), new SessionContextImpl(null,
            conn));
        EasyMock.verify(conn);
    }

}