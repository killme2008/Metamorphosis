package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.utils.test.ConcurrentTestCase;
import com.taobao.metamorphosis.utils.test.ConcurrentTestTask;


public class ConcurrentLRUHashMapUnitTest {

    private ConcurrentLRUHashMap<Long, Long> map;


    @Before
    public void setUp() {
        this.map = new ConcurrentLRUHashMap<Long, Long>(100);
    }


    @Test
    public void concurrentTest() {
        final AtomicLong counter = new AtomicLong(0);
        ConcurrentTestCase testCase = new ConcurrentTestCase(100, 100000, new ConcurrentTestTask() {

            @Override
            public void run(int index, int times) throws Exception {
                long v = counter.incrementAndGet();
                ConcurrentLRUHashMapUnitTest.this.map.put(v, v);

            }
        });
        testCase.start();
        System.out.println(testCase.getDurationInMillis());
        assertEquals(100, this.map.size());
    }
}
