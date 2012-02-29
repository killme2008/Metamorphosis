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
package com.taobao.metamorphosis.gregor.slave;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.gregor.slave.OrderedThreadPoolExecutor.TasksQueue;


//import com.taobao.metamorphosis.notifyadapter.OrderedThreadPoolExecutor.TasksQueue;

public class OrderedThreadPoolExecutorUnitTest {
    private OrderedThreadPoolExecutor executor;
    int threadCount = 10;
    private Connection conn;

    private TasksQueue taskQueue;


    @Before
    public void setUp() {
        this.taskQueue = new TasksQueue();
        this.executor = new OrderedThreadPoolExecutor(this.threadCount, this.threadCount);
        this.conn = EasyMock.createMock(Connection.class);
        EasyMock.makeThreadSafe(this.conn, true);
        EasyMock.expect(this.conn.getAttribute(this.executor.TASKS_QUEUE)).andReturn(this.taskQueue).anyTimes();
        EasyMock.replay(this.conn);
    }


    @After
    public void tearDown() {
        EasyMock.verify(this.conn);
        this.executor.shutdown();
    }


    @Test
    public void testExecuteInOrderMultiTask() throws Exception {
        final LinkedList<Integer> numbers = new LinkedList<Integer>();
        final CountDownLatch latch = new CountDownLatch(10000);
        for (int i = 0; i < 10000; i++) {
            final int x = i;
            this.executor.execute(new IoEvent() {

                @Override
                public void run() {
                    numbers.offer(x);
                    latch.countDown();
                }


                @Override
                public IoCatalog getIoCatalog() {
                    return new IoCatalog(OrderedThreadPoolExecutorUnitTest.this.conn, null);
                }

            });
        }
        latch.await();
        assertEquals(10000, numbers.size());
        for (int i = 0; i < 10000; i++) {
            assertEquals(i, (int) numbers.poll());
        }
    }


    @Test
    public void testExecuteInOrder() throws Exception {
        final LinkedList<Integer> numbers = new LinkedList<Integer>();
        final CountDownLatch latch = new CountDownLatch(2);
        // 第一个任务sleep 3秒后添加元素1
        this.executor.execute(new IoEvent() {

            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    numbers.offer(1);
                    latch.countDown();
                }
                catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }


            @Override
            public IoCatalog getIoCatalog() {
                return new IoCatalog(OrderedThreadPoolExecutorUnitTest.this.conn, null);
            }

        });
        // 第二个任务立即添加元素2
        this.executor.execute(new IoEvent() {

            @Override
            public void run() {
                numbers.offer(2);
                latch.countDown();
            }


            @Override
            public IoCatalog getIoCatalog() {
                return new IoCatalog(OrderedThreadPoolExecutorUnitTest.this.conn, null);
            }

        });
        latch.await();
        // 确保1先加入
        assertEquals(1, (int) numbers.poll());
        assertEquals(2, (int) numbers.poll());
        assertNull(numbers.poll());
    }

}