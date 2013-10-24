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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.store.MessageStore.Segment;
import com.taobao.metamorphosis.server.store.MessageStore.SegmentList;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metamorphosis.utils.MessageUtils;
import com.taobao.metamorphosis.utils.MessageUtils.DecodedMessage;
import com.taobao.metamorphosis.utils.test.ConcurrentTestCase;
import com.taobao.metamorphosis.utils.test.ConcurrentTestTask;


public class MessageStoreUnitTest {
    private static final int MSG_COUNT = 10;
    private MessageStore messageStore;
    private final String topic = "test";
    private final int partition = 1;
    private MetaConfig metaConfig;
    private DeletePolicy deletePolicy;
    private IdWorker idWorker;


    @Before
    public void setUp() throws Exception {
        final String tmpPath = System.getProperty("java.io.tmpdir");
        this.metaConfig = new MetaConfig();
        this.metaConfig.setDataPath(tmpPath);
        final PutCommand cmd1 = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        this.metaConfig.setUnflushThreshold(1);
        // 限制存10个消息就roll文件
        this.metaConfig.setMaxSegmentSize(MessageUtils.makeMessageBuffer(1, cmd1).capacity() * MSG_COUNT);
        this.idWorker = new IdWorker(0);
        this.clearTopicPartDir();
        this.deletePolicy = new DiscardDeletePolicy();
        ((DiscardDeletePolicy) this.deletePolicy).setMaxReservedTime(Integer.MAX_VALUE);
        this.messageStore = new MessageStore(this.topic, this.partition, this.metaConfig, this.deletePolicy);
    }


    @After
    public void clearTopicPartDir() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.close();
        }
        final File topicPartDir =
                new File(this.metaConfig.getDataPath() + File.separator + this.topic + "-" + this.partition);
        if (topicPartDir.exists()) {
            for (final File file : topicPartDir.listFiles()) {
                file.delete();
            }
        }
    }


    @Test
    public void testSegmentContants() throws Exception {
        final File file = new File("testSegmentContants.test");
        if (!file.exists()) {
            file.createNewFile();
        }
        final Segment segment = new Segment(0, file);
        try {
            segment.fileMessageSet.setHighWaterMarker(1024);
            assertTrue(segment.contains(0));
            assertFalse(segment.contains(1024));
            assertFalse(segment.contains(1025));
            assertFalse(segment.contains(2048));
            assertTrue(segment.contains(1));
            assertTrue(segment.contains(100));
            assertTrue(segment.contains(512));
            assertTrue(segment.contains(1023));
        }
        finally {
            if (segment != null) {
                segment.fileMessageSet.close();
            }
            file.delete();
        }
    }


    @Test
    public void testAppendSegmentDeleteSegment() throws Exception {
        final SegmentList segmentList = new SegmentList();
        assertEquals(0, segmentList.contents.get().length);
        final File file = new File("testAppendSegmentDeleteSegment.test");
        if (!file.exists()) {
            file.createNewFile();
        }
        final Segment segment1 = new Segment(0, file);
        final Segment segment2 = new Segment(1024, file);
        final Segment segment3 = new Segment(2048, file);
        try {
            segmentList.append(segment1);
            assertEquals(1, segmentList.contents.get().length);
            assertSame(segment1, segmentList.first());
            assertSame(segment1, segmentList.last());
            segmentList.append(segment2);
            assertEquals(2, segmentList.contents.get().length);
            assertSame(segment1, segmentList.first());
            assertSame(segment2, segmentList.last());
            segmentList.append(segment3);
            assertEquals(3, segmentList.contents.get().length);
            assertSame(segment1, segmentList.first());
            assertSame(segment3, segmentList.last());

            segmentList.delete(segment1);
            assertEquals(2, segmentList.contents.get().length);
            assertSame(segment2, segmentList.first());
            assertSame(segment3, segmentList.last());

            segmentList.delete(segment3);
            assertEquals(1, segmentList.contents.get().length);
            assertSame(segment2, segmentList.first());
            assertSame(segment2, segmentList.last());
            // delete not existing
            segmentList.delete(segment3);
            assertEquals(1, segmentList.contents.get().length);
            assertSame(segment2, segmentList.first());
            assertSame(segment2, segmentList.last());

            segmentList.delete(segment2);
            assertEquals(0, segmentList.contents.get().length);
            assertNull(segmentList.first());
            assertNull(segmentList.last());
        }
        finally {
            if (segment1 != null) {
                segment1.fileMessageSet.close();
            }
            file.delete();
        }
    }


    @Test
    public void testAppendMessages() throws Exception {
        final PutCommand cmd1 = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        final PutCommand cmd2 = new PutCommand(this.topic, this.partition, "world".getBytes(), null, 0, 0);
        final long id1 = this.idWorker.nextId();
        final long id2 = this.idWorker.nextId();
        this.messageStore.append(id1, cmd1, new AppendCallback() {
            @Override
            public void appendComplete(final Location location) {
                if (0 != location.getOffset()) {
                    throw new RuntimeException();
                }
            }
        });
        this.messageStore.flush();
        final long size = this.messageStore.getSegments().last().size();
        this.messageStore.append(id2, cmd2, new AppendCallback() {
            @Override
            public void appendComplete(final Location location) {
                assertEquals(size, location.getOffset());
                if (size != location.getOffset()) {
                    throw new RuntimeException();
                }
            }
        });
        this.messageStore.flush();

        this.assertMessages(id1, id2);

    }


    @Test
    public void testConcurrentAppendMessages() throws Exception {
        System.out.println("Begin concurrent test....");
        this.metaConfig.setMaxSegmentSize(1024 * 1024 * 16);
        ConcurrentTestCase testCase = new ConcurrentTestCase(80, 1000, new ConcurrentTestTask() {

            @Override
            public void run(int index, int times) throws Exception {
                final PutCommand cmd =
                        new PutCommand(MessageStoreUnitTest.this.topic, MessageStoreUnitTest.this.partition,
                            new byte[1024], null, 0, 0);
                final long id = MessageStoreUnitTest.this.idWorker.nextId();
                final CountDownLatch latch = new CountDownLatch(1);
                MessageStoreUnitTest.this.messageStore.append(id, cmd, new AppendCallback() {
                    @Override
                    public void appendComplete(final Location location) {
                        if (location.getOffset() < 0) {
                            throw new RuntimeException();
                        }
                        else {
                            latch.countDown();
                        }
                    }

                });
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    // ignore
                }
            }
        });
        testCase.start();
        System.out.println("Appended 80000 messages,cost:" + testCase.getDurationInMillis() / 1000 + " seconds");
        assertEquals(80000, this.messageStore.getMessageCount());

    }


    @Test
    // 创建一个指定offset的MessageStore，并写消息
    public void testAppendMessages_toOffset() throws Exception {

        // 先清空一下
        this.clearTopicPartDir();

        final int offset = 2048;
        this.messageStore = new MessageStore(this.topic, this.partition, this.metaConfig, this.deletePolicy, offset);
        final PutCommand cmd1 = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        final PutCommand cmd2 = new PutCommand(this.topic, this.partition, "world".getBytes(), null, 0, 0);
        final long id1 = this.idWorker.nextId();
        final long id2 = this.idWorker.nextId();
        this.messageStore.append(id1, cmd1, new AppendCallback() {
            @Override
            public void appendComplete(final Location location) {
                assertEquals(2048, location.getOffset());
                if (2048 != location.getOffset()) {
                    throw new RuntimeException();
                }
            }
        });
        this.messageStore.flush();
        final long size = this.messageStore.getSegments().last().size();
        this.messageStore.append(id2, cmd2, new AppendCallback() {

            @Override
            public void appendComplete(final Location location) {
                assertEquals(offset + size, location.getOffset());
                if (offset + size != location.getOffset()) {
                    throw new RuntimeException();
                }
            }
        });
        this.messageStore.flush();

        final File partDir = this.messageStore.getPartitionDir();
        final File[] logs = partDir.listFiles();
        assertEquals(1, logs.length);
        assertTrue(logs[0].exists());
        assertEquals(this.messageStore.nameFromOffset(offset), logs[0].getName());
        final FileChannel channel = new RandomAccessFile(logs[0], "rw").getChannel();
        try {
            final ByteBuffer buf = ByteBuffer.allocate((int) channel.size());
            while (buf.hasRemaining()) {
                channel.read(buf);
            }
            buf.flip();

            final DecodedMessage decodedMessage1 = MessageUtils.decodeMessage(this.topic, buf.array(), 0);
            final DecodedMessage decodedMessage2 =
                    MessageUtils.decodeMessage(this.topic, buf.array(), decodedMessage1.newOffset);
            final Message msg1 = new Message(this.topic, "hello".getBytes());
            MessageAccessor.setId(msg1, id1);
            final Message msg2 = new Message(this.topic, "world".getBytes());
            MessageAccessor.setId(msg2, id2);
            assertEquals(msg1, decodedMessage1.message);
            assertEquals(msg2, decodedMessage2.message);
        }
        finally {
            channel.close();
        }
    }


    @Test
    public void testGetNearestOffset() throws Exception {
        final SegmentList segmentList = new SegmentList();
        assertEquals(0, segmentList.contents.get().length);
        final File file = new File("testGetNearestOffset.test");
        if (!file.exists()) {
            file.createNewFile();
        }
        final Segment segment1 = new Segment(0, file);
        segment1.fileMessageSet.setHighWaterMarker(1024);
        final Segment segment2 = new Segment(1024, file);
        segment2.fileMessageSet.setHighWaterMarker(1024);
        final Segment segment3 = new Segment(2048, file);
        segment3.fileMessageSet.setHighWaterMarker(1024);
        final Segment segment4 = new Segment(3072, file);
        segment4.fileMessageSet.setHighWaterMarker(1024);

        segmentList.append(segment1);
        segmentList.append(segment2);
        segmentList.append(segment3);
        segmentList.append(segment4);

        try {
            assertEquals(0, this.messageStore.getNearestOffset(-100, segmentList));
            assertEquals(0, this.messageStore.getNearestOffset(0, segmentList));
            assertEquals(0, this.messageStore.getNearestOffset(100, segmentList));
            assertEquals(0, this.messageStore.getNearestOffset(1023, segmentList));
            assertEquals(1024, this.messageStore.getNearestOffset(1024, segmentList));
            assertEquals(1024, this.messageStore.getNearestOffset(2000, segmentList));
            assertEquals(2048, this.messageStore.getNearestOffset(2048, segmentList));
            assertEquals(2048, this.messageStore.getNearestOffset(2049, segmentList));
            assertEquals(2048, this.messageStore.getNearestOffset(2536, segmentList));
            assertEquals(3072, this.messageStore.getNearestOffset(3072, segmentList));
            assertEquals(3072, this.messageStore.getNearestOffset(3073, segmentList));
            assertEquals(3072 + 1024, this.messageStore.getNearestOffset(4096, segmentList));
            assertEquals(3072 + 1024, this.messageStore.getNearestOffset(16 * 1024, segmentList));
        }
        finally {
            file.delete();
            segment1.fileMessageSet.close();
            segment2.fileMessageSet.close();
            segment3.fileMessageSet.close();
            segment4.fileMessageSet.close();
        }
    }


    @Test
    public void testFindSegment() throws Exception {
        final SegmentList segmentList = new SegmentList();
        assertEquals(0, segmentList.contents.get().length);
        final File file = new File("testFindSegment.test");
        if (!file.exists()) {
            file.createNewFile();
        }
        final Segment segment1 = new Segment(0, file);
        segment1.fileMessageSet.setHighWaterMarker(1024);
        final Segment segment2 = new Segment(1024, file);
        segment2.fileMessageSet.setHighWaterMarker(1024);
        final Segment segment3 = new Segment(2048, file);
        segment3.fileMessageSet.setHighWaterMarker(1024);
        final Segment segment4 = new Segment(3072, file);
        segment4.fileMessageSet.setHighWaterMarker(1024);

        segmentList.append(segment1);
        segmentList.append(segment2);
        segmentList.append(segment3);
        segmentList.append(segment4);
        try {
            assertSame(segment1, this.messageStore.findSegment(segmentList.view(), 0));
            assertSame(segment1, this.messageStore.findSegment(segmentList.view(), 1));
            assertSame(segment1, this.messageStore.findSegment(segmentList.view(), 1023));
            assertSame(segment2, this.messageStore.findSegment(segmentList.view(), 1024));
            assertSame(segment2, this.messageStore.findSegment(segmentList.view(), 1536));
            assertSame(segment3, this.messageStore.findSegment(segmentList.view(), 2048));
            assertSame(segment3, this.messageStore.findSegment(segmentList.view(), 2049));
            assertSame(segment3, this.messageStore.findSegment(segmentList.view(), 3000));
            assertSame(segment4, this.messageStore.findSegment(segmentList.view(), 3072));
            assertSame(segment4, this.messageStore.findSegment(segmentList.view(), 3073));
            assertNull(this.messageStore.findSegment(segmentList.view(), 4097));
            assertNull(this.messageStore.findSegment(segmentList.view(), 4098));
            assertNull(this.messageStore.findSegment(segmentList.view(), 16 * 1024));
            try {
                this.messageStore.findSegment(segmentList.view(), -1);
                fail();
            }
            catch (final ArrayIndexOutOfBoundsException e) {
                assertTrue(true);
            }
        }
        finally {
            file.delete();
            segment1.fileMessageSet.close();
            segment2.fileMessageSet.close();
            segment3.fileMessageSet.close();
            segment4.fileMessageSet.close();
        }

    }


    @Test
    public void testAppendMessagesRoll() throws Exception {
        final PutCommand req = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        final AtomicLong size = new AtomicLong(0);
        for (int i = 0; i < MSG_COUNT; i++) {
            if (i == 0) {
                this.messageStore.append(this.idWorker.nextId(), req, new AppendCallback() {

                    @Override
                    public void appendComplete(final Location location) {
                        assertEquals(0, location.getOffset());
                        if (0 != location.getOffset()) {
                            throw new RuntimeException();
                        }
                    }
                });
                this.messageStore.flush();
                size.set(this.messageStore.getSegments().last().size());
            }
            else {
                final int j = i;
                this.messageStore.append(this.idWorker.nextId(), req, new AppendCallback() {

                    @Override
                    public void appendComplete(final Location location) {
                        assertEquals(size.get() * j, location.getOffset());
                        if (size.get() * j != location.getOffset()) {
                            throw new RuntimeException();
                        }
                    }
                });
                this.messageStore.flush();
                // assertEquals(size * i,
                // this.messageStore.append(this.idWorker.nextId(),
                // req).getOffset());
            }

        }

        final File partDir = this.messageStore.getPartitionDir();
        final File[] logs = partDir.listFiles();
        assertEquals(2, logs.length);
        Arrays.sort(logs, new Comparator<File>() {

            @Override
            public int compare(final File o1, final File o2) {
                final long l =
                        Long.parseLong(o1.getName().split("\\.")[0]) - Long.parseLong(o2.getName().split("\\.")[0]);
                return l == 0 ? 0 : l > 0 ? 1 : -1;
            }

        });
        assertEquals(this.messageStore.nameFromOffset(0), logs[0].getName());
        assertEquals(this.messageStore.nameFromOffset(this.metaConfig.getMaxSegmentSize()), logs[1].getName());

        final FileChannel channel1 = new RandomAccessFile(logs[0], "rw").getChannel();
        final FileChannel channel2 = new RandomAccessFile(logs[1], "rw").getChannel();
        try {
            assertEquals(this.metaConfig.getMaxSegmentSize(), channel1.size());
            assertEquals(0, channel2.size());
        }
        finally {
            channel1.close();
            channel2.close();
        }
    }


    @Test
    public void testAppendMessagesCloseRecover() throws Exception {
        final PutCommand req = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        final AtomicLong size = new AtomicLong(0);
        for (int i = 0; i < 100; i++) {
            if (i == 0) {
                this.messageStore.append(this.idWorker.nextId(), req, new AppendCallback() {

                    @Override
                    public void appendComplete(final Location location) {
                        assertEquals(0, location.getOffset());
                        if (0 != location.getOffset()) {
                            throw new RuntimeException();
                        }
                    }
                });
                this.messageStore.flush();
                size.set(this.messageStore.getSegments().last().size());
            }
            else {
                final int j = i;
                this.messageStore.append(this.idWorker.nextId(), req, new AppendCallback() {

                    @Override
                    public void appendComplete(final Location location) {
                        assertEquals(size.get() * j, location.getOffset());
                        if (size.get() * j != location.getOffset()) {
                            throw new RuntimeException();
                        }
                    }
                });
                this.messageStore.flush();
            }
        }
        this.messageStore.flush();
        final Segment[] segments = this.messageStore.getSegments().view();
        final int oldSegmentCount = segments.length;
        final String lastSegmentName = segments[segments.length - 1].file.getName();
        final long lastSegmentSize = segments[segments.length - 1].size();

        this.messageStore.close();
        // recover
        this.messageStore = new MessageStore(this.topic, this.partition, this.metaConfig, this.deletePolicy);

        final Segment[] newSegments = this.messageStore.getSegments().view();
        assertEquals(oldSegmentCount, newSegments.length);
        assertEquals(lastSegmentName, newSegments[newSegments.length - 1].file.getName());
        assertEquals(lastSegmentSize, newSegments[newSegments.length - 1].size());

        Segment prev = null;
        for (final Segment s : newSegments) {
            if (prev != null) {
                assertEquals(s.start, prev.start + prev.size());
            }
            prev = s;
        }
    }


    @Test
    public void testSlice() throws Exception {
        final PutCommand req = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        for (int i = 0; i < MSG_COUNT; i++) {
            this.messageStore.append(this.idWorker.nextId(), req, null);
        }
        FileMessageSet subSet = (FileMessageSet) this.messageStore.slice(0, this.metaConfig.getMaxSegmentSize() - 10);
        assertEquals(0, subSet.getOffset());
        assertEquals(this.metaConfig.getMaxSegmentSize() - 10, subSet.getSizeInBytes());

        subSet = (FileMessageSet) this.messageStore.slice(0, this.metaConfig.getMaxSegmentSize() + 10);
        assertEquals(0, subSet.getOffset());
        assertEquals(this.metaConfig.getMaxSegmentSize(), subSet.getSizeInBytes());

        subSet =
                (FileMessageSet) this.messageStore.slice(this.metaConfig.getMaxSegmentSize() - 10,
                    this.metaConfig.getMaxSegmentSize() + 10);
        assertEquals(this.metaConfig.getMaxSegmentSize() - 10, subSet.getOffset());
        assertEquals(10, subSet.getSizeInBytes());

        assertNull(this.messageStore.slice(10000, 1024));
    }


    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testSliceIndexOutOfBounds() throws Exception {
        this.messageStore.slice(-1, 1024);
    }


    @Test
    public void testRunDeletePolicy() throws Exception {
        ((DiscardDeletePolicy) this.deletePolicy).setMaxReservedTime(1000);
        final PutCommand req = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        for (int i = 0; i < MSG_COUNT; i++) {
            this.messageStore.append(this.idWorker.nextId(), req, null);
        }

        final File partDir = this.messageStore.getPartitionDir();
        assertEquals(2, partDir.listFiles().length);
        assertEquals(2, this.messageStore.getSegments().view().length);
        Thread.sleep(1500);
        this.messageStore.runDeletePolicy();
        assertEquals(1, partDir.listFiles().length);
        assertEquals(1, this.messageStore.getSegments().view().length);

        Thread.sleep(1500);
        this.messageStore.runDeletePolicy();
        assertEquals(1, partDir.listFiles().length);
        assertEquals(1, this.messageStore.getSegments().view().length);
    }


    @Test
    public void testAppendMany() throws Exception {
        final PutCommand cmd1 = new PutCommand(this.topic, this.partition, "hello".getBytes(), null, 0, 0);
        final PutCommand cmd2 = new PutCommand(this.topic, this.partition, "world".getBytes(), null, 0, 0);
        final long id1 = this.idWorker.nextId();
        final long id2 = this.idWorker.nextId();

        final List<Long> ids = new ArrayList<Long>();
        ids.add(id1);
        ids.add(id2);
        final List<PutCommand> cmds = new ArrayList<PutCommand>();
        cmds.add(cmd1);
        cmds.add(cmd2);

        this.messageStore.append(ids, cmds, new AppendCallback() {

            @Override
            public void appendComplete(final Location location) {
                assertEquals(0, location.getOffset());
                if (0 != location.getOffset()) {
                    throw new RuntimeException();
                }
            }
        });
        this.messageStore.flush();

        this.assertMessages(id1, id2);
    }


    private void assertMessages(final long id1, final long id2) throws FileNotFoundException, IOException,
    InvalidMessageException {
        final File partDir = this.messageStore.getPartitionDir();
        final File[] logs = partDir.listFiles();
        assertEquals(1, logs.length);
        assertEquals(this.messageStore.nameFromOffset(0), logs[0].getName());
        final FileChannel channel = new RandomAccessFile(logs[0], "rw").getChannel();
        try {
            final ByteBuffer buf = ByteBuffer.allocate((int) channel.size());
            while (buf.hasRemaining()) {
                channel.read(buf);
            }
            buf.flip();

            final DecodedMessage decodedMessage1 = MessageUtils.decodeMessage(this.topic, buf.array(), 0);
            final DecodedMessage decodedMessage2 =
                    MessageUtils.decodeMessage(this.topic, buf.array(), decodedMessage1.newOffset);
            final Message msg1 = new Message(this.topic, "hello".getBytes());
            MessageAccessor.setId(msg1, id1);
            final Message msg2 = new Message(this.topic, "world".getBytes());
            MessageAccessor.setId(msg2, id2);
            assertEquals(msg1, decodedMessage1.message);
            assertEquals(msg2, decodedMessage2.message);
        }
        finally {
            channel.close();
        }
    }

}