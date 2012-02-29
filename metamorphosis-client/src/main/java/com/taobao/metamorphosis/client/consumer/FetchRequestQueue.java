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

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 稳定排序的delay queue，线程安全
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
class FetchRequestQueue {
    private final LinkedList<FetchRequest> queue = new LinkedList<FetchRequest>();
    private final Lock lock = new ReentrantLock();
    private final Condition available = this.lock.newCondition();


    public FetchRequest take() throws InterruptedException {
        this.lock.lockInterruptibly();
        try {
            for (;;) {
                final FetchRequest first = this.queue.peek();
                if (first == null) {
                    this.available.await();
                }
                else {
                    final long delay = first.getDelay(TimeUnit.NANOSECONDS);
                    if (delay > 0) {
                        final long tl = this.available.awaitNanos(delay);
                    }
                    else {
                        final FetchRequest x = this.queue.poll();
                        assert x != null;
                        if (this.queue.size() != 0) {
                            this.available.signalAll(); // wake up other takers
                        }
                        return x;

                    }
                }
            }
        }
        finally {
            this.lock.unlock();
        }
    }


    public void offer(final FetchRequest request) {
        this.lock.lock();
        try {
            final FetchRequest first = this.queue.peek();
            this.queue.offer(request);
            Collections.sort(this.queue);
            if (first == null || request.compareTo(first) < 0) {
                this.available.signalAll();
            }
        }
        finally {
            this.lock.unlock();
        }
    }


    public int size() {
        this.lock.lock();
        try {
            return this.queue.size();
        }
        finally {
            this.lock.unlock();
        }
    }

}