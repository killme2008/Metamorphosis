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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


public class RoundRobinPartitionSelectorUnitTest {

    private RoundRobinPartitionSelector selector;


    @Before
    public void setUp() {
        this.selector = new RoundRobinPartitionSelector();
    }


    @Test(expected = MetaClientException.class)
    public void testSelect_EmptyList() throws Exception {
        assertNull(this.selector.getPartition("test", null, null));
    }


    @Test
    public void testSelectRoundRobin() throws Exception {
        final Partition p1 = new Partition("0-1");
        final Partition p2 = new Partition("0-2");
        final Partition p3 = new Partition("0-3");

        final List<Partition> list = new ArrayList<Partition>();
        list.add(p1);
        list.add(p2);
        list.add(p3);

        assertSame(p2, this.selector.getPartition("test", list, null));
        assertSame(p3, this.selector.getPartition("test", list, null));
        assertSame(p1, this.selector.getPartition("test", list, null));
        assertSame(p2, this.selector.getPartition("test", list, null));
        assertSame(p3, this.selector.getPartition("test", list, null));
        assertSame(p1, this.selector.getPartition("test", list, null));
    }

}