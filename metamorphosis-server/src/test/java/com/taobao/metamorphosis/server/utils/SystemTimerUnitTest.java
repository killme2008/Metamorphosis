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
package com.taobao.metamorphosis.server.utils;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.taobao.metamorphosis.utils.test.ConcurrentTestCase;
import com.taobao.metamorphosis.utils.test.ConcurrentTestTask;


public class SystemTimerUnitTest {

    @Test
    public void testPerformance() {
        System.out.println(testSystem() / testSystemTimer());

    }


    @Test
    public void testCurrentTimeMillis() throws Exception {
        final long start = SystemTimer.currentTimeMillis();
        Thread.sleep(1000);
        final long end = SystemTimer.currentTimeMillis();
        System.out.println(end - start);
        assertEquals(1000L, end - start, 50L);
    }


    private static long testSystem() {
        final AtomicLong result = new AtomicLong(0);
        final ConcurrentTestCase testCase = new ConcurrentTestCase(50, 400000, new ConcurrentTestTask() {

            public void run(final int index, final int times) throws Exception {
                result.addAndGet(System.currentTimeMillis());

            }
        });
        testCase.start();
        System.out.println("System:" + result.get());
        return testCase.getDurationInMillis();
    }


    private static long testSystemTimer() {
        final AtomicLong result = new AtomicLong(0);
        final ConcurrentTestCase testCase = new ConcurrentTestCase(50, 400000, new ConcurrentTestTask() {

            public void run(final int index, final int times) throws Exception {
                result.addAndGet(SystemTimer.currentTimeMillis());

            }
        });
        testCase.start();
        System.out.println("SystemTimer:" + result.get());
        return testCase.getDurationInMillis();
    }
}