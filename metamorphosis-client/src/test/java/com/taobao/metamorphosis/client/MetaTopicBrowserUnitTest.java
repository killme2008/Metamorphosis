package com.taobao.metamorphosis.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;


public class MetaTopicBrowserUnitTest {

    private MetaTopicBrowser browser;

    private List<Partition> partitions;

    private final String topic = "test";

    private final int maxSize = 1024;

    private final long timeoutInMills = 1000L;

    private MessageConsumer consumer;

    private IMocksControl control;


    @Before
    public void setUp() {
        this.control = EasyMock.createControl();
        this.consumer = this.control.createMock(MessageConsumer.class);
        this.partitions = new ArrayList<Partition>();
        for (int i = 0; i < 3; i++) {
            this.partitions.add(new Partition("0-" + i));
        }
        this.browser =
                new MetaTopicBrowser(this.topic, this.maxSize, this.timeoutInMills, this.consumer, this.partitions);
    }


    private byte[] createMessageBuffer() {
        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN + 5);
        buf.putInt(5);// msg length
        buf.putInt(CheckSum.crc32("hello".getBytes())); // checksum
        buf.putLong(9999); // id
        buf.putInt(0); // flag

        buf.position(MessageUtils.HEADER_LEN);
        buf.put("hello".getBytes());
        return buf.array();
    }


    private void assertMsg(Message msg) {
        assertNotNull(msg);
        assertEquals(9999L, msg.getId());
        assertEquals("test", msg.getTopic());
        assertFalse(msg.hasAttribute());
        assertEquals(0, MessageAccessor.getFlag(msg));
        assertEquals("hello", new String(msg.getData()));
    }


    @Test
    public void testIteratorInSamePartition() throws Exception {
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-0"), 0L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-0"), 25L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        this.control.replay();
        Iterator<Message> it = this.browser.iterator();
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        this.control.verify();
        MetaTopicBrowser.Itr mit = (MetaTopicBrowser.Itr) it;
        assertEquals(2, mit.partitions.size());
        assertFalse(mit.partitions.contains(new Partition("0-0")));
    }


    @Test
    public void testIteratorMoveOnPartition() throws Exception {
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-0"), 0L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        EasyMock.expect(
            this.consumer.get(this.topic, new Partition("0-0"), 25L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(null);
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-1"), 0L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        this.control.replay();
        Iterator<Message> it = this.browser.iterator();
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        this.control.verify();
        MetaTopicBrowser.Itr mit = (MetaTopicBrowser.Itr) it;
        assertEquals(1, mit.partitions.size());
        assertFalse(mit.partitions.contains(new Partition("0-0")));
        assertFalse(mit.partitions.contains(new Partition("0-1")));
    }


    @Test
    public void testIteratorToEnd() throws Exception {
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-0"), 0L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        EasyMock.expect(
            this.consumer.get(this.topic, new Partition("0-0"), 25L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(null);
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-1"), 0L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-1"), 25L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(null).once();
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-2"), 0L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(new MessageIterator(this.topic, this.createMessageBuffer()))
                .once();
        EasyMock
        .expect(
            this.consumer.get(this.topic, new Partition("0-2"), 25L, this.maxSize, this.timeoutInMills,
                TimeUnit.MILLISECONDS)).andReturn(null).once();
        this.control.replay();
        Iterator<Message> it = this.browser.iterator();
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        if (it.hasNext()) {
            this.assertMsg(it.next());
        }
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
        this.control.verify();
        MetaTopicBrowser.Itr mit = (MetaTopicBrowser.Itr) it;
        assertEquals(0, mit.partitions.size());
        try {
            it.next();
            fail();
        }
        catch (NoSuchElementException e) {

        }
    }
}
