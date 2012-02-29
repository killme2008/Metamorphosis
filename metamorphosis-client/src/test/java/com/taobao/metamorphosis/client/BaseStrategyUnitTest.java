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
package com.taobao.metamorphosis.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;


public abstract class BaseStrategyUnitTest {
    public List<String> createPartitions(final String topic, final int num) {
        final List<String> rt = new ArrayList<String>();
        for (int i = 0; i < num; i++) {
            rt.add(topic + "-" + i);
        }
        return rt;
    }


    public void assertInclude(final List<String> parts, final String... expects) {
        System.out.println(parts);
        for (final String expect : expects) {
            assertTrue(parts.contains(expect));
        }
        assertEquals(parts.size(), expects.length);
    }


    public List<String> createConsumers(final int num) throws Exception {
        final List<String> rt = new ArrayList<String>();
        for (int i = 0; i < num; i++) {
            rt.add("consumer-" + i);
        }
        return rt;
    }
}