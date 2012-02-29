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
package com.taobao.metamorphosis.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-7-22 ÏÂÎç04:30:28
 */

public class PartitionTest {

    @Test
    public void testCompareTo() {
        assertTrue(new Partition(2, 0).compareTo(new Partition(2, 0)) == 0);
        assertTrue(new Partition(2, 1).compareTo(new Partition(2, 0)) == 1);
        assertTrue(new Partition(2, 0).compareTo(new Partition(2, 1)) == -1);
        assertTrue(new Partition(3, 0).compareTo(new Partition(2, 0)) == 1);
        assertTrue(new Partition(2, 0).compareTo(new Partition(3, 0)) == -1);
        assertTrue(new Partition(3, 0).compareTo(new Partition(2, 5)) == 1);
        assertTrue(new Partition(2, 5).compareTo(new Partition(3, 0)) == -1);

        assertTrue(new Partition(2, 5).compareTo(new Partition(10, 5)) == -1);
        assertTrue(new Partition(10, 5).compareTo(new Partition(2, 5)) == 1);
        assertTrue(new Partition(10, 0).compareTo(new Partition(2, 5)) == 1);
        assertTrue(new Partition(2, 5).compareTo(new Partition(10, 0)) == -1);

        assertTrue(new Partition(102, 15).compareTo(new Partition(203, 2)) == -1);
        assertTrue(new Partition(203, 2).compareTo(new Partition(102, 15)) == 1);

    }


    @Test
    public void testCompareTo_order() {
        final List<Partition> partitions = new ArrayList<Partition>();
        for (int i = 21; i >= 0; i--) {
            for (int j = 14; j >= 0; j--) {
                partitions.add(new Partition(i, j));
            }
        }
        Collections.shuffle(partitions);
        System.out.println(partitions);

        Collections.sort(partitions);
        System.out.println(partitions);

        for (int i = 0; i < partitions.size(); i++) {
            if (i == partitions.size() - 1) {
                return;
            }
            assertTrue(partitions.get(i).compareTo(partitions.get(i + 1)) == -1);
            assertTrue(partitions.get(i).getBrokerId() <= partitions.get(i + 1).getBrokerId());
            if (partitions.get(i).getBrokerId() == partitions.get(i + 1).getBrokerId()) {
                assertTrue(partitions.get(i).getPartition() < partitions.get(i + 1).getPartition());
            }
        }

    }


    @Test
    public void testNewRandomPartitonByString() {
        final Partition partition = new Partition("-1--1");
        assertEquals(Partition.RandomPartiton, partition);
        assertEquals(Partition.RandomPartiton.getBrokerId(), partition.getBrokerId());
        assertEquals(Partition.RandomPartiton.getPartition(), partition.getPartition());

    }


    @Test
    public void testAckRollbackReset() {
        final Partition partition = new Partition("0-0");
        assertFalse(partition.isRollback());
        assertTrue(partition.isAcked());
        assertTrue(partition.isAutoAck());

        partition.setAutoAck(false);
        assertFalse(partition.isRollback());
        assertFalse(partition.isAcked());
        assertFalse(partition.isAutoAck());

        partition.ack();
        assertFalse(partition.isRollback());
        assertTrue(partition.isAcked());
        assertFalse(partition.isAutoAck());
        try {
            partition.rollback();
            fail();
        }
        catch (final IllegalStateException e) {
            assertEquals("Could not rollback acked partition", e.getMessage());
        }

        partition.reset();
        assertFalse(partition.isRollback());
        assertFalse(partition.isAcked());
        assertFalse(partition.isAutoAck());

        partition.rollback();
        assertTrue(partition.isRollback());
        assertFalse(partition.isAcked());
        assertFalse(partition.isAutoAck());
        try {
            partition.ack();
            fail();
        }
        catch (final IllegalStateException e) {
            assertEquals("Could not ack rollbacked partition", e.getMessage());
        }

        partition.setAutoAck(true);
        assertFalse(partition.isRollback());
        assertTrue(partition.isAcked());
        assertTrue(partition.isAutoAck());
    }
}