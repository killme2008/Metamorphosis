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
package com.taobao.metamorphosis.server.transaction.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.store.FileMessageSet;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.transaction.BaseTransactionUnitTest;
import com.taobao.metamorphosis.server.transaction.TransactionRecoveryListener;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore.Tx;
import com.taobao.metamorphosis.server.utils.XIDGenerator;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.test.ConcurrentTestCase;
import com.taobao.metamorphosis.utils.test.ConcurrentTestTask;


public class JournalTransactionStoreUnitTest extends BaseTransactionUnitTest {

    @Test
    public void testAddAddRollBackCloseRecover() throws Exception {
        final LocalTransactionId xid = new LocalTransactionId("test", 1);
        final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid, 0, 1), null);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg2".getBytes(), xid, 0, 2), null);

        final Tx tx = this.transactionStore.getInflyTx(xid);
        assertNotNull(tx);
        final PutCommand[] commands = tx.getRequests();
        assertNotNull(commands);
        assertEquals(2, commands.length);
        store.flush();
        // 还没有写入
        assertEquals(0, store.getSizeInBytes());

        // rollback
        this.transactionStore.rollback(xid);
        assertNull(this.transactionStore.getInflyTx(xid));
        store.flush();
        // 回滚，当然没有写入
        assertEquals(0, store.getSizeInBytes());

        this.tearDown();
        this.init(this.path);
        assertTrue(this.journalStore.getCurrDataFile().getLength() > 0);
        assertNull(this.transactionStore.getInflyTx(xid));
        assertEquals(0, this.messageStoreManager.getOrCreateMessageStore("topic1", 2).getSizeInBytes());

    }


    @Test
    public void testAddAddCloseRecover() throws Exception {
        final LocalTransactionId xid1 = new LocalTransactionId("test", 1);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid1, 0, 1), null);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg2".getBytes(), xid1, 0, 2), null);

        final LocalTransactionId xid2 = new LocalTransactionId("test", 2);
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid2, 0, 1), null);

        this.tearDown();
        this.init(this.path);
        assertTrue(this.journalStore.getCurrDataFile().getLength() > 0);

        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        // 确认消息没有写入
        assertEquals(0, store.getSizeInBytes());

        // 确认TX还存在
        final Tx tx1 = this.transactionStore.getInflyTx(xid1);
        assertNotNull(tx1);
        PutCommand[] commands = tx1.getRequests();
        assertNotNull(commands);
        assertEquals(2, commands.length);

        final Tx tx2 = this.transactionStore.getInflyTx(xid2);
        assertNotNull(tx2);
        commands = tx2.getRequests();
        assertNotNull(commands);
        assertEquals(1, commands.length);

        // recover后，都回滚了
        this.transactionStore.recover(null);
        assertNull(this.transactionStore.getInflyTx(xid1));
        assertNull(this.transactionStore.getInflyTx(xid2));
        // 确认消息还是没有写入
        assertEquals(0, store.getSizeInBytes());
    }


    @Test
    public void testAddAddCommitCloseRecover() throws Exception {
        final LocalTransactionId xid = new LocalTransactionId("test", 1);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid, 0, 1), null);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg2".getBytes(), xid, 0, 2), null);

        final Tx tx = this.transactionStore.getInflyTx(xid);
        assertNotNull(tx);
        final PutCommand[] commands = tx.getRequests();
        assertNotNull(commands);
        assertEquals(2, commands.length);
        store.flush();
        // 还没有写入
        assertEquals(0, store.getSizeInBytes());

        // rollback
        this.transactionStore.commit(xid, false);
        assertNull(this.transactionStore.getInflyTx(xid));
        store.flush();
        // 写入消息
        assertTrue(store.getSizeInBytes() > 0);

        this.tearDown();
        this.init(this.path);
        assertTrue(this.journalStore.getCurrDataFile().getLength() > 0);
        assertNull(this.transactionStore.getInflyTx(xid));

        // 重新打开store
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.assertMessages(store);
    }


    private void assertMessages(final MessageStore store) throws IOException, InvalidMessageException {
        final FileMessageSet msgSet = (FileMessageSet) store.slice(0, 1024);
        final ByteBuffer buf = ByteBuffer.allocate((int) msgSet.getSizeInBytes());
        msgSet.read(buf, 0);
        final MessageIterator it = new MessageIterator("topic1", buf.array());
        int count = 0;
        while (it.hasNext()) {
            final Message msg = it.next();
            assertTrue(new String(msg.getData()).startsWith("msg"));
            count++;
        }
        assertEquals(2, count);
    }


    @Test
    public void testBeginCommitCloseRecover() throws Exception {
        final LocalTransactionId xid = new LocalTransactionId("test", 1);
        this.transactionStore.commit(xid, false);
        assertNull(this.transactionStore.getInflyTx(xid));
        this.tearDown();
        this.init(this.path);
        assertNull(this.transactionStore.getInflyTx(xid));
    }


    @Test
    public void testAddAddPrepareCommitCloseRecover() throws Exception {
        final XATransactionId xid = XIDGenerator.createXID(99);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid, 0, 1), null);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg2".getBytes(), xid, 0, 2), null);
        assertNotNull(this.transactionStore.getInflyTx(xid));
        assertNull(this.transactionStore.getPreparedTx(xid));
        // prepare
        this.transactionStore.prepare(xid);
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNotNull(this.transactionStore.getPreparedTx(xid));
        store.flush();
        // 确认还未写入
        assertEquals(0, store.getSizeInBytes());

        // commit
        this.transactionStore.commit(xid, true);
        store.flush();
        assertTrue(store.getSizeInBytes() > 0);
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNull(this.transactionStore.getPreparedTx(xid));

        // close and reopen
        this.tearDown();
        this.init(this.path);
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        assertTrue(store.getSizeInBytes() > 0);
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNull(this.transactionStore.getPreparedTx(xid));
        this.assertMessages(store);

    }


    @Test
    public void testAddAddPrepareRollbackCloseRecover() throws Exception {
        final XATransactionId xid = XIDGenerator.createXID(99);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid, 0, 1), null);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg2".getBytes(), xid, 0, 2), null);
        assertNotNull(this.transactionStore.getInflyTx(xid));
        assertNull(this.transactionStore.getPreparedTx(xid));
        // prepare
        this.transactionStore.prepare(xid);
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNotNull(this.transactionStore.getPreparedTx(xid));
        store.flush();
        // 确认还未写入
        assertEquals(0, store.getSizeInBytes());

        // rollback
        this.transactionStore.rollback(xid);
        store.flush();
        assertEquals(0, store.getSizeInBytes());
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNull(this.transactionStore.getPreparedTx(xid));

        // close and reopen
        this.tearDown();
        this.init(this.path);
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        assertEquals(0, store.getSizeInBytes());
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNull(this.transactionStore.getPreparedTx(xid));

    }


    @Test
    public void testAddAddPrepareCloseRecover() throws Exception {
        final XATransactionId xid = XIDGenerator.createXID(99);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        final PutCommand cmd1 = new PutCommand("topic1", 2, "msg1".getBytes(), xid, 0, 1);
        this.transactionStore.addMessage(store, 1, cmd1, null);
        final PutCommand cmd2 = new PutCommand("topic1", 2, "msg2".getBytes(), xid, 0, 2);
        this.transactionStore.addMessage(store, 1, cmd2, null);

        // prepare
        this.transactionStore.prepare(xid);
        assertNull(this.transactionStore.getInflyTx(xid));
        assertNotNull(this.transactionStore.getPreparedTx(xid));
        store.flush();
        // 确认还未写入
        assertEquals(0, store.getSizeInBytes());

        // close and reopen
        this.tearDown();
        this.init(this.path);
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        assertEquals(0, store.getSizeInBytes());
        assertNull(this.transactionStore.getInflyTx(xid));
        // 仍然处于prepare状态
        assertNotNull(this.transactionStore.getPreparedTx(xid));
        // 确认操作存在
        final Tx tx = this.transactionStore.getPreparedTx(xid);
        assertNotNull(tx);
        final PutCommand[] commands = tx.getRequests();
        assertNotNull(commands);
        assertEquals(2, commands.length);
        for (final PutCommand cmd : commands) {
            assertTrue(cmd.equals(cmd1) || cmd.equals(cmd2));
        }

        // recover
        this.transactionStore.recover(new TransactionRecoveryListener() {

            @Override
            public void recover(final XATransactionId id, final PutCommand[] addedMessages) {
                assertEquals(xid, id);
                assertArrayEquals(commands, addedMessages);

            }
        });
    }


    @Test
    public void testCheckpoint() throws Exception {
        // 事务1
        final LocalTransactionId xid1 = new LocalTransactionId("session1", 1);
        final MessageStore store1 = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store1, 1, new PutCommand("topic1", 2, ("msg" + 2).getBytes(), xid1, 0, 1),
            null);

        // 事务2
        final LocalTransactionId xid2 = new LocalTransactionId("session2", 1);
        final MessageStore store2 = this.messageStoreManager.getOrCreateMessageStore("topic1", 3);
        this.transactionStore.addMessage(store2, 1, new PutCommand("topic1", 3, ("msg" + 3).getBytes(), xid2, 0, 1),
            null);

        // 事务3，已经提交
        final LocalTransactionId xid3 = new LocalTransactionId("session3", 1);
        final MessageStore store3 = this.messageStoreManager.getOrCreateMessageStore("topic1", 0);
        this.transactionStore.addMessage(store3, 1, new PutCommand("topic1", 0, ("msg" + 0).getBytes(), xid3, 0, 1),
            null);
        this.transactionStore.commit(xid3, false);

        // 查找checkpoint，应当为事务1
        final JournalLocation location = this.transactionStore.checkpoint();
        final Tx tx = this.transactionStore.getInflyTx(xid1);
        assertEquals(location, tx.getLocation());
    }


    @Test
    public void concurrentTest() throws Exception {
        final Random rand = new Random();
        final AtomicInteger gen = new AtomicInteger();
        final ConcurrentTestCase testCase = new ConcurrentTestCase(100, 1000, new ConcurrentTestTask() {

            @Override
            public void run(final int index, final int times) throws Exception {
                final int id = gen.incrementAndGet();
                final LocalTransactionId xid = new LocalTransactionId("test", id);
                for (int j = 0; j < rand.nextInt(3) + 1; j++) {
                    final int partition = rand.nextInt(10);
                    final MessageStore store =
                            JournalTransactionStoreUnitTest.this.messageStoreManager.getOrCreateMessageStore("topic1",
                                partition % 10);
                    JournalTransactionStoreUnitTest.this.transactionStore.addMessage(store, 1, new PutCommand("topic1",
                        partition, ("msg" + id).getBytes(), xid, 0, 1), null);
                }
                if (id % 100 == 0) {
                    JournalTransactionStoreUnitTest.this.journalStore.checkpoint();
                }
                // commit
                JournalTransactionStoreUnitTest.this.transactionStore.commit(xid, false);

            }
        });

        testCase.start();

        System.out.println("测试时间：" + testCase.getDurationInMillis() + "ms");

        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", i);
            assertTrue(store.getSizeInBytes() > 0);
        }
        assertEquals(0, this.transactionStore.getActiveTransactionCount());

        // 关闭打开
        this.tearDown();
        final long start = System.currentTimeMillis();
        this.init(this.path);
        System.out.println("恢复话费时间:" + (System.currentTimeMillis() - start) + "ms");
        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", i);
            assertTrue(store.getSizeInBytes() > 0);
        }
        assertEquals(0, this.transactionStore.getActiveTransactionCount());
    }


    @Test
    public void testAddManyRollJournal() throws Exception {
        final Random rand = new Random();
        this.tearDown();
        final int oldSize = JournalStore.MAX_FILE_SIZE;
        JournalStore.MAX_FILE_SIZE = 512;
        try {
            this.init(this.path);
            for (int i = 0; i < 10000; i++) {
                final LocalTransactionId xid = new LocalTransactionId("test", i);
                // 随即添加几条消息
                for (int j = 0; j < rand.nextInt(3) + 1; j++) {
                    final int partition = rand.nextInt(10);
                    final MessageStore store =
                            this.messageStoreManager.getOrCreateMessageStore("topic1", partition % 10);
                    this.transactionStore.addMessage(store, 1,
                        new PutCommand("topic1", partition, ("msg" + i).getBytes(), xid, 0, 1), null);
                }
                // commit
                this.transactionStore.commit(xid, false);

            }
            // 确认文件roll了
            assertTrue(this.journalStore.getCurrDataFile().getNumber() > 1);
            assertEquals(1, this.journalStore.getDataFiles().size());
        }
        finally {
            JournalStore.MAX_FILE_SIZE = oldSize;
        }

    }


    @Test
    public void testAddManyCheckpointRecover() throws Exception {
        final Random rand = new Random();
        for (int i = 0; i < 10000; i++) {
            final LocalTransactionId xid = new LocalTransactionId("test", i);

            // 随即添加几条消息
            for (int j = 0; j < rand.nextInt(3) + 1; j++) {
                final int partition = rand.nextInt(10);
                final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", partition % 10);
                this.transactionStore.addMessage(store, 1, new PutCommand("topic1", partition, ("msg" + i).getBytes(),
                    xid, 0, 1), null);
            }
            if (i % 100 == 0) {
                this.journalStore.checkpoint();
            }
            // commit
            this.transactionStore.commit(xid, false);
            if (i % 77 == 0) {
                this.journalStore.checkpoint();
            }
        }

        // 关闭打开
        this.tearDown();
        final long start = System.currentTimeMillis();
        this.init(this.path);
        System.out.println("恢复话费时间:" + (System.currentTimeMillis() - start));
        for (int i = 0; i < 10; i++) {
            final MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", i);
            assertTrue(store.getSizeInBytes() > 0);
        }
        assertEquals(0, this.transactionStore.getActiveTransactionCount());
    }


    @Test
    public void testAddAddCommit_CloseReplayAppend_CloseRecover() throws Exception {
        final LocalTransactionId xid = new LocalTransactionId("test", 1);
        MessageStore store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg1".getBytes(), xid, 0, 1), null);
        this.transactionStore.addMessage(store, 1, new PutCommand("topic1", 2, "msg2".getBytes(), xid, 0, 2), null);
        assertNotNull(this.transactionStore.getInflyTx(xid));

        // commit
        this.transactionStore.commit(xid, false);
        store.flush();
        final long sizeInBytes = store.getSizeInBytes();
        assertTrue(sizeInBytes > 0);
        this.assertMessages(store);

        this.tearDown();
        // delete data
        System.out.println("删除目录:" + this.path + File.separator + store.getDescription());
        FileUtils.deleteDirectory(new File(this.path + File.separator + store.getDescription()));
        this.init(this.path);

        // 应该重放插入数据了,确认数据正确
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        store.flush();
        assertTrue(store.getSizeInBytes() > 0);
        assertEquals(sizeInBytes, store.getSizeInBytes());
        this.assertMessages(store);
        assertNull(this.transactionStore.getInflyTx(xid));

        // 再次关闭打开，状态正确
        this.tearDown();
        this.init(this.path);
        store = this.messageStoreManager.getOrCreateMessageStore("topic1", 2);
        assertTrue(store.getSizeInBytes() > 0);
        assertEquals(sizeInBytes, store.getSizeInBytes());
        this.assertMessages(store);
        assertNull(this.transactionStore.getInflyTx(xid));

    }
}