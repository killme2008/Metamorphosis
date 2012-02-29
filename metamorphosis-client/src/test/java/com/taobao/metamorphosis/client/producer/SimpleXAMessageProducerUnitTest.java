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
package com.taobao.metamorphosis.client.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;


public class SimpleXAMessageProducerUnitTest {

    @Test
    public void testIntersect() {
        final List<Set<String>> sets = new ArrayList<Set<String>>();
        final Set<String> set1 = new HashSet<String>();
        set1.add("meta://localhost:8123");
        set1.add("meta://localhost:8124");
        set1.add("meta://localhost:8125");

        final Set<String> set2 = new HashSet<String>();
        set2.add("meta://localhost:8121");
        set2.add("meta://localhost:8122");
        set2.add("meta://localhost:8123");

        final Set<String> set3 = new HashSet<String>();
        set3.add("meta://localhost:8121");
        set3.add("meta://localhost:8123");
        set3.add("meta://localhost:8125");

        sets.add(set1);
        sets.add(set2);
        sets.add(set3);
        final Set<String> rt = SimpleXAMessageProducer.intersect(sets);
        assertNotNull(rt);
        assertEquals(1, rt.size());
        assertTrue(rt.contains("meta://localhost:8123"));
    }


    @Test
    public void testNoIntersect() {
        final List<Set<String>> sets = new ArrayList<Set<String>>();
        final Set<String> set1 = new HashSet<String>();
        set1.add("meta://localhost:8123");
        set1.add("meta://localhost:8124");
        set1.add("meta://localhost:8125");

        final Set<String> set2 = new HashSet<String>();
        set2.add("meta://localhost:8121");
        set2.add("meta://localhost:8122");
        set2.add("meta://localhost:8123");

        final Set<String> set3 = new HashSet<String>();
        set3.add("meta://localhost:8121");
        set3.add("meta://localhost:8125");

        sets.add(set1);
        sets.add(set2);
        sets.add(set3);
        final Set<String> rt = SimpleXAMessageProducer.intersect(sets);
        assertNotNull(rt);
        assertTrue(rt.isEmpty());
    }
}