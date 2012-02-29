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

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.server.utils.MetaConfig;


public class StatsManagerUnitTest {

    private StatsManager statsManager;

    private final String group = "boyan-test";


    @Before
    public void setUp() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.getStatTopicSet().add("test");
        metaConfig.getStatTopicSet().add("test2");
        this.statsManager = new StatsManager(metaConfig, null, null);
        this.statsManager.init();
    }


    @After
    public void tearDown() {
        this.statsManager.dispose();
    }


    @Test
    public void testStatPut() {
        assertEquals(0, this.statsManager.getCmdPuts());
        this.statsManager.statsPut("test", "1-0", 1);
        this.statsManager.statsPut("test2", "2-0", 4);
        assertEquals(5, this.statsManager.getCmdPuts());
    }


    @Test
    public void testStatGet() {
        assertEquals(0, this.statsManager.getCmdGets());
        this.statsManager.statsGet("test", this.group, 1);
        this.statsManager.statsGet("test2", this.group, 4);
        assertEquals(5, this.statsManager.getCmdGets());
    }


    @Test
    public void testStatPutFailed() {
        assertEquals(0, this.statsManager.getCmdPutFailed());
        this.statsManager.statsPutFailed("test", "", 1);
        this.statsManager.statsPutFailed("test2", "", 4);
        assertEquals(5, this.statsManager.getCmdPutFailed());
    }


    @Test
    public void testStatOffset() {
        assertEquals(0, this.statsManager.getCmdOffsets());
        this.statsManager.statsOffset("test", this.group, 1);
        this.statsManager.statsOffset("test2", this.group, 4);
        assertEquals(5, this.statsManager.getCmdOffsets());
    }


    @Test
    public void testStatGetMiss() {
        assertEquals(0, this.statsManager.getCmdGetMiss());
        this.statsManager.statsGetMiss("test", this.group, 1);
        this.statsManager.statsGetMiss("test2", this.group, 4);
        assertEquals(5, this.statsManager.getCmdGetMiss());
    }


    @Test
    public void testStatGetFailed() {
        assertEquals(0, this.statsManager.getCmdGetFailed());
        this.statsManager.statsGetFailed("test", this.group, 1);
        this.statsManager.statsGetFailed("test2", this.group, 4);
        assertEquals(5, this.statsManager.getCmdGetFailed());
    }


    @Test
    public void testAppend() {
        StringBuilder sb = new StringBuilder();
        this.statsManager.append(sb, "key1", 1);
        this.statsManager.append(sb, "key2", 2L);
        this.statsManager.append(sb, "key3", "test");
        assertEquals("key1 1\r\nkey2 2\r\nkey3 test\r\n", sb.toString());
    }
}