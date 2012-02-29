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
package com.taobao.metamorphosis.server.stats;

import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.metamorphosis.utils.MetaStatLog;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-9-16 ÏÂÎç12:51:19
 */

public class RealTimeStatUnitTest {
    private RealTimeStat realTimeStat;
    static final String APP_NAME = "_test";

    static final String format = "Count=%s,Value=%s,Value/Count=%s";

    static String oldFormat = null;


    @Before
    public void setup() {
        oldFormat = MetaStatLog.OUTPUT_FORMAT;
        MetaStatLog.startRealTimeStat = true;
        MetaStatLog.OUTPUT_FORMAT = format;
        this.realTimeStat = new RealTimeStat();
    }

    static String STAR = "*";


    @Test
    public void testStatOneKey() {
        final String key1 = "key1";

        MetaStatLog.addStat(APP_NAME, key1);
        Assert.assertEquals(String.format(format, 1, 0, 0), this.realTimeStat.getStatResult(key1));

        for (int i = 0; i < 10000; i++) {
            MetaStatLog.addStat(APP_NAME, key1);
        }
        Assert.assertEquals(String.format(format, 10001, 0, 0), this.realTimeStat.getStatResult(key1));

        // test add value
        MetaStatLog.addStatValue2(APP_NAME, key1, 20000L);
        Assert.assertEquals(String.format(format, 10002, 20000L, 2), this.realTimeStat.getStatResult(key1));
    }


    @Test
    public void testStatTwoKeys() {
        final String key1 = "key1";
        final String key2 = "key2";

        MetaStatLog.addStat(APP_NAME, key1, key2);
        Assert.assertEquals(String.format(format, 1, 0, 0), this.realTimeStat.getStatResult(key1, key2));

        for (int i = 0; i < 10000; i++) {
            MetaStatLog.addStat(APP_NAME, key1, key2);
        }
        Assert.assertEquals(String.format(format, 10001, 0, 0), this.realTimeStat.getStatResult(key1, key2));

        // test add value
        this.realTimeStat.resetStat();
        MetaStatLog.addStatValue2(APP_NAME, key1, key2, 100);
        Assert.assertEquals(String.format(format, 1, 100, 100), this.realTimeStat.getStatResult(key1, key2));
        // add more
        for (int i = 0; i < 10000; i++) {
            MetaStatLog.addStatValue2(APP_NAME, key1, key2, 100);
        }
        Assert.assertEquals(String.format(format, 10001, 1000100, 100), this.realTimeStat.getStatResult(key1, key2));
    }


    @Test
    public void testStatThreeKeys() {
        final String key1 = "key1";
        final String key2 = "key2";
        final String key3 = "key3";

        MetaStatLog.addStat(APP_NAME, key1, key2, key3);
        Assert.assertEquals(String.format(format, 1, 0, 0), this.realTimeStat.getStatResult(key1, key2, key3));

        for (int i = 0; i < 10000; i++) {
            MetaStatLog.addStat(APP_NAME, key1, key2, key3);
        }
        Assert.assertEquals(String.format(format, 10001, 0, 0), this.realTimeStat.getStatResult(key1, key2, key3));

        // test add value
        this.realTimeStat.resetStat();
        MetaStatLog.addStatValue2(APP_NAME, key1, key2, key3, 100);
        Assert.assertEquals(String.format(format, 1, 100, 100), this.realTimeStat.getStatResult(key1, key2, key3));
        // add more
        for (int i = 0; i < 10000; i++) {
            MetaStatLog.addStatValue2(APP_NAME, key1, key2, key3, 100);
        }
        Assert.assertEquals(String.format(format, 10001, 1000100, 100),
            this.realTimeStat.getStatResult(key1, key2, key3));
    }


    @Test
    public void testThreadSafe() {
        final String key1 = "key1";
        final String key2 = "key2";
        final String key3 = "key3";

        final CountDownLatch latch = new CountDownLatch(1000);
        for (int i = 0; i < 1000; i++) {
            new ConcurrentStatThread(key1, key2, key3, latch).start();
        }
        try {
            latch.await();
        }
        catch (final InterruptedException e) {
            throw new RuntimeException(e);

        }
        Assert.assertEquals(String.format(format, 100000, 0, 0), this.realTimeStat.getStatResult(key1, key2, key3));
    }


    @Ignore
    public void testStatStar() {
        final String key1 = "key1";

        MetaStatLog.addStat(APP_NAME, key1, "test", "test");
        MetaStatLog.addStat(APP_NAME, key1, "test", "test3");
        MetaStatLog.addStat(APP_NAME, key1, "test1", "test1");
        MetaStatLog.addStat(APP_NAME, key1, "test2", "test2");
        Assert.assertEquals(String.format(format, 4, 0, "invalid"), this.realTimeStat.getStatResult(key1, STAR, STAR));
    }


    @Ignore
    public void testAutoReset() throws Exception {
        // final long oldInterval =
        // ConfigFactory.getStatConfig().getRealTimeStatInterval();
        // ConfigFactory.getStatConfig().setRealTimeStatInterval(1000);
        final String key1 = "key1";
        final String key2 = "key2";
        final String key3 = "key3";

        MetaStatLog.addStat(APP_NAME, key1, key2, key3);
        Assert.assertEquals(String.format(format, 1, 0, 0), this.realTimeStat.getStatResult(key1, key2, key3));

        this.realTimeStat.start();
        // waiting for reset action
        Thread.sleep(2000);
        Assert.assertEquals(String.format(format, 0, 0, "invalid"), this.realTimeStat.getStatResult(key1, key2, key3));
        this.realTimeStat.stop();
        // ConfigFactory.getStatConfig().setRealTimeStatInterval(oldInterval);
    }

    class ConcurrentStatThread extends Thread {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        CountDownLatch latch;


        public ConcurrentStatThread(final String key1, final String key2, final String key3, final CountDownLatch latch) {
            super();
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = key3;
            this.latch = latch;
        }


        @Override
        public void run() {
            for (int i = 0; i < 100; i++) {
                MetaStatLog.addStat(APP_NAME, this.key1, this.key2, this.key3);
            }
            this.latch.countDown();
        }
    }


    @After
    public void tearDown() {
        MetaStatLog.OUTPUT_FORMAT = oldFormat;
        // ConfigFactory.getStatConfig().setRealTimeStatMap(new HashMap<String,
        // Map<String, Map<String, Boolean>>>());
        this.realTimeStat.resetStat();
    }
}