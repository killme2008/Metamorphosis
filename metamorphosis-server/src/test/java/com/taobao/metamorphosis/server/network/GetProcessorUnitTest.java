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
package com.taobao.metamorphosis.server.network;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.DataCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.store.MessageSet;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.utils.MessageUtils;


public class GetProcessorUnitTest extends BaseProcessorUnitTest {
    private GetProcessor getProcessor;
    final String topic = "GetProcessorUnitTest";
    private final String group = "boyan-test";


    @Before
    public void setUp() {
        this.mock();
        this.getProcessor = new GetProcessor(this.commandProcessor, null);
    }


    @Test
    public void testHandleRequestNoStore() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(null);
        this.conn.response(new BooleanCommand(HttpStatus.NotFound, "The topic `" + this.topic
            + "` in partition `" + partition + "` is not exists", opaque));
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        final GetCommand request = new GetCommand(this.topic, this.group, partition, opaque, 1024 * 1024, opaque);
        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestInvalidMaxSize() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = -1;
        this.metaConfig.setMaxTransferSize(1024);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(
            new MessageStore(this.topic, partition, this.metaConfig, null));
        this.conn
        .response(new BooleanCommand(HttpStatus.BadRequest, "Bad request,invalid max size:" + maxSize, opaque));
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        final GetCommand request = new GetCommand(this.topic, this.group, partition, opaque, maxSize, opaque);
        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(0, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestEquelsMaxOffsetSliceNull() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        EasyMock.expect(store.slice(offset, maxSize)).andReturn(null);
        this.conn.response(new BooleanCommand(HttpStatus.NotFound, "Could not find message at position "
                + offset, opaque));
        EasyMock.expect(store.getMaxOffset()).andReturn(offset);
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestGreatThanMaxOffsetSliceNull() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        EasyMock.expect(store.slice(offset, maxSize)).andReturn(null);
        this.conn.response(new BooleanCommand(HttpStatus.NotFound, "Could not find message at position "
                + offset, opaque));
        EasyMock.expect(store.getMaxOffset()).andReturn(offset - 1);
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestMaxOffset() throws Exception {
        // 从实际最大的offset开始订阅
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = Long.MAX_VALUE;
        final long realMaxOffset = 100;
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        EasyMock.expect(store.slice(offset, maxSize)).andReturn(null);
        this.conn.response(new BooleanCommand(HttpStatus.Moved, String.valueOf(realMaxOffset), opaque));
        EasyMock.expect(store.getMaxOffset()).andReturn(realMaxOffset);
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(1, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestArrayIndexOutOfBounds() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;
        final long newOffset = 512;
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        EasyMock.expect(store.slice(offset, maxSize)).andThrow(new ArrayIndexOutOfBoundsException());
        EasyMock.expect(store.getNearestOffset(offset)).andReturn(newOffset);
        this.conn.response(new BooleanCommand(HttpStatus.Moved, String.valueOf(newOffset), opaque));
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(1, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(1, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestNormal() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        final MessageSet set = this.mocksControl.createMock(MessageSet.class);
        EasyMock.expect(store.slice(offset, maxSize)).andReturn(set);
        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        set.write(request, this.sessionContext);
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(0, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }

    public static class AcceptMessageFilter implements ConsumerMessageFilter {

        @Override
        public boolean accept(String group, Message message) {
            return true;
        }
    }

    @Test
    public void testHandleRequestNormalWithMessageFilter() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;
        final ByteBuffer msgBuf =
                MessageUtils.makeMessageBuffer(999, new PutCommand(this.topic, partition, "hello world".getBytes(),
                    null, 0, opaque));
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        final MessageSet set = this.mocksControl.createMock(MessageSet.class);
        EasyMock.expect(store.slice(offset, maxSize)).andReturn(set);
        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        set.read((ByteBuffer) EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {

            @Override
            public Void answer() throws Throwable {
                final Object[] args = EasyMock.getCurrentArguments();
                ByteBuffer buf = (ByteBuffer) args[0];
                buf.put(msgBuf);
                return null;
            }

        });
        this.consumerFilterManager = this.mocksControl.createMock(ConsumerFilterManager.class);
        EasyMock.expect(this.consumerFilterManager.findFilter(this.topic, this.group))
        .andReturn(
            new AcceptMessageFilter());
        this.commandProcessor.setConsumerFilterManager(this.consumerFilterManager);
        this.conn.response(new DataCommand(msgBuf.array(), opaque, true));
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(0, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestNormalWithMessageFilter_NotAccept() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;
        final ByteBuffer msgBuf =
                MessageUtils.makeMessageBuffer(999, new PutCommand(this.topic, partition, "hello world".getBytes(),
                    null, 0, opaque));
        final MessageStore store = this.mocksControl.createMock(MessageStore.class);
        EasyMock.expect(this.storeManager.getMessageStore(this.topic, partition)).andReturn(store);
        final MessageSet set = this.mocksControl.createMock(MessageSet.class);
        EasyMock.expect(store.slice(offset, maxSize)).andReturn(set);
        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        set.read((ByteBuffer) EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {

            @Override
            public Void answer() throws Throwable {
                final Object[] args = EasyMock.getCurrentArguments();
                ByteBuffer buf = (ByteBuffer) args[0];
                buf.put(msgBuf);
                return null;
            }

        });
        this.consumerFilterManager = this.mocksControl.createMock(ConsumerFilterManager.class);
        EasyMock.expect(this.consumerFilterManager.findFilter(this.topic, this.group)).andReturn(
            new ConsumerMessageFilter() {

                @Override
                public boolean accept(String group, Message message) {
                    return false;
                }
            });
        this.commandProcessor.setConsumerFilterManager(this.consumerFilterManager);
        this.conn.response(new BooleanCommand(HttpStatus.Moved, String.valueOf(offset + msgBuf.capacity()), opaque));
        EasyMock.expectLastCall();
        this.mocksControl.replay();

        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
        assertEquals(0, this.statsManager.getCmdGetMiss());
        assertEquals(1, this.statsManager.getCmdGets());
        assertEquals(0, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testHandleRequestPartitionClosed() throws Exception {
        final int partition = 1;
        final int opaque = 0;
        final int maxSize = 1024;
        final long offset = 10;

        this.metaConfig.setTopics(Arrays.asList(this.topic));
        this.metaConfig.closePartitions(this.topic, partition, partition);
        final GetCommand request = new GetCommand(this.topic, this.group, partition, offset, maxSize, opaque);
        this.conn.response(new BooleanCommand(HttpStatus.Forbidden, "Partition["
                + this.metaConfig.getBrokerId() + "-" + request.getPartition() + "] has been closed", request.getOpaque()));
        EasyMock.expectLastCall();

        this.mocksControl.replay();

        this.getProcessor.handleRequest(request, this.conn);
        this.mocksControl.verify();
    }

    //
    // @Test
    // public void testHandleRequestPartitionClosedSlave() throws Exception {
    // final int partition = 1;
    // final int opaque = 0;
    // final int maxSize = 1024;
    // final long offset = 10;
    //
    // this.metaConfig.setTopics(Arrays.asList(this.topic));
    // this.metaConfig.closePartitions(this.topic, partition, partition);
    //
    // final MessageStore store =
    // this.mocksControl.createMock(MessageStore.class);
    // EasyMock.expect(this.storeManager.getMessageStore(this.topic,
    // partition)).andReturn(store);
    // final MessageSet set = this.mocksControl.createMock(MessageSet.class);
    // EasyMock.expect(store.slice(offset, maxSize)).andReturn(set);
    //
    // final GetCommand request =
    // new GetCommand(this.topic, this.metaConfig.getSlaveGroup(), partition,
    // offset, maxSize, opaque);
    // // this.conn.response(new BooleanCommand(request.getOpaque(),
    // // HttpStatus.Forbidden, "partition["
    // // + this.metaConfig.getBrokerId() + "-" + request.getPartition() +
    // // "] have been closed"));
    // set.write(request, this.sessionContext);
    // EasyMock.expectLastCall();
    //
    // this.mocksControl.replay();
    //
    // this.getProcessor.handleRequest(request, this.conn);
    // this.mocksControl.verify();
    // assertEquals(0, this.statsManager.getCmdGetMiss());
    // assertEquals(1, this.statsManager.getCmdGets());
    // assertEquals(0, this.statsManager.getCmdOffsets());
    // }

}