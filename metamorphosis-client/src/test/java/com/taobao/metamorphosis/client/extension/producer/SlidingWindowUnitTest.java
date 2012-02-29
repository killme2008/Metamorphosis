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
package com.taobao.metamorphosis.client.extension.producer;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-12-29 ÏÂÎç5:18:13
 */

public class SlidingWindowUnitTest {

    @Before
    public void setUp() throws Exception {
    }


    @Test
    public void testTryAcquireByLengthInt() {
        SlidingWindow window = new SlidingWindow(3);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2));

        window.releaseByLenth(4096 * 2);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2));
    }


    @Test
    public void testTryAcquireByLengthIntLongTimeUnit() throws InterruptedException {
        SlidingWindow window = new SlidingWindow(3);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));

        window.releaseByLenth(4096 * 2);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));
    }


    @Test
    public void testReleaseByLenth() {

    }


    @Test
    public void testGetWindowsSize() {
        SlidingWindow window = new SlidingWindow(3);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2));
        Assert.assertTrue(window.getWindowsSize() == 3);
    }

}