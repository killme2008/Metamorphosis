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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.utils.test.ConcurrentTestCase;
import com.taobao.metamorphosis.utils.test.ConcurrentTestTask;


public class FetchRequestQueueUnitTest {
    private FetchRequestQueue fetchRequestQueue;


    @Before
    public void setUp() {
        this.fetchRequestQueue = new FetchRequestQueue();
    }


    @Test
    public void testOfferTakeSize() throws Exception {
        this.fetchRequestQueue.offer(new FetchRequest(0));
        final FetchRequest delayed = new FetchRequest(1000);
        this.fetchRequestQueue.offer(delayed);
        this.fetchRequestQueue.offer(new FetchRequest(0));

        assertEquals(3, this.fetchRequestQueue.size());
        assertNotSame(delayed, this.fetchRequestQueue.take());
        assertNotSame(delayed, this.fetchRequestQueue.take());
        assertSame(delayed, this.fetchRequestQueue.take());
        assertEquals(0, this.fetchRequestQueue.size());
    }


    @Test
    public void testSamePriority() throws Exception {
        final FetchRequest first = new FetchRequest(1000);
        final FetchRequest second = new FetchRequest(1000);
        final FetchRequest third = new FetchRequest(1000);
        final FetchRequest four = new FetchRequest(1000);
        this.fetchRequestQueue.offer(first);
        this.fetchRequestQueue.offer(second);
        this.fetchRequestQueue.offer(third);
        this.fetchRequestQueue.offer(four);
        assertSame(first, this.fetchRequestQueue.take());
        assertSame(second, this.fetchRequestQueue.take());
        assertSame(third, this.fetchRequestQueue.take());
        assertSame(four, this.fetchRequestQueue.take());
    }


    @Test
    public void testDifferentPriority() throws Exception {
        final FetchRequest first = new FetchRequest(1000);
        final FetchRequest second = new FetchRequest(3000);
        this.fetchRequestQueue.offer(first);
        this.fetchRequestQueue.offer(second);
        long start = System.currentTimeMillis();
        assertSame(first, this.fetchRequestQueue.take());
        assertEquals(System.currentTimeMillis() - start, 1000L, 10L);
        start = System.currentTimeMillis();
        assertSame(second, this.fetchRequestQueue.take());
        assertEquals(System.currentTimeMillis() - start, 2000L, 10L);
    }


    @Test
    public void concurrentTest() {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicBoolean shutdown = new AtomicBoolean(false);
        new Thread() {
            @Override
            public void run() {
                while (!shutdown.get()) {
                    try {
                        Thread.sleep(1000);
                    }
                    catch (final InterruptedException e) {

                    }
                    System.out.println(counter.get() + " " + FetchRequestQueueUnitTest.this.fetchRequestQueue.size()
                        + " ");
                }
            }

        }.start();

        final ConcurrentTestCase testCase = new ConcurrentTestCase(500, 10000, new ConcurrentTestTask() {

            @Override
            public void run(final int index, final int times) throws Exception {
                FetchRequestQueueUnitTest.this.fetchRequestQueue.offer(new FetchRequest(times % 3));
                try {
                    FetchRequestQueueUnitTest.this.fetchRequestQueue.take();
                    counter.incrementAndGet();
                }
                catch (final InterruptedException e) {

                }
            }
        });
        testCase.start();
        assertEquals(5000000, counter.get());
        assertEquals(0, this.fetchRequestQueue.size());
        System.out.println(testCase.getDurationInMillis());
        shutdown.set(true);
    }


    @Test
    public void testTakeWaitingOfferedDelayed() throws Exception {
        final AtomicReference<FetchRequest> offered = new AtomicReference<FetchRequest>();
        final AtomicBoolean done = new AtomicBoolean();
        new Thread() {
            @Override
            public void run() {
                try {
                    offered.set(FetchRequestQueueUnitTest.this.fetchRequestQueue.take());
                    done.set(true);
                }
                catch (final InterruptedException e) {
                }
            }
        }.start();
        Thread.sleep(1000);

        final FetchRequest request = new FetchRequest(1000);
        this.fetchRequestQueue.offer(request);
        while (!done.get()) {
            Thread.sleep(500);
        }
        assertSame(offered.get(), request);
    }


    @Test
    public void testTakeWaitingOfferedNotDelay() throws Exception {
        final AtomicReference<FetchRequest> offered = new AtomicReference<FetchRequest>();
        final AtomicBoolean done = new AtomicBoolean();
        new Thread() {
            @Override
            public void run() {
                try {
                    offered.set(FetchRequestQueueUnitTest.this.fetchRequestQueue.take());
                    done.set(true);
                }
                catch (final InterruptedException e) {
                }
            }
        }.start();
        Thread.sleep(1000);

        final FetchRequest request = new FetchRequest(0);
        this.fetchRequestQueue.offer(request);
        while (!done.get()) {
            Thread.sleep(500);
        }
        assertSame(offered.get(), request);
    }


    @Test
    public void testTakeNotWaitingOfferdNotDelay() throws Exception {
        final FetchRequest request = new FetchRequest(0);
        this.fetchRequestQueue.offer(request);
        final AtomicReference<FetchRequest> offered = new AtomicReference<FetchRequest>();
        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    offered.set(FetchRequestQueueUnitTest.this.fetchRequestQueue.take());
                }
                catch (final InterruptedException e) {
                }
            }
        };
        thread.start();
        thread.join();
        assertSame(offered.get(), request);
    }


    @Test
    public void testTakeNotWaitingOfferdDelayed() throws Exception {
        final FetchRequest request = new FetchRequest(1000);
        this.fetchRequestQueue.offer(request);
        final AtomicReference<FetchRequest> offered = new AtomicReference<FetchRequest>();
        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    offered.set(FetchRequestQueueUnitTest.this.fetchRequestQueue.take());
                }
                catch (final InterruptedException e) {
                }
            }
        };
        thread.start();
        thread.join();
        assertSame(offered.get(), request);
    }
}