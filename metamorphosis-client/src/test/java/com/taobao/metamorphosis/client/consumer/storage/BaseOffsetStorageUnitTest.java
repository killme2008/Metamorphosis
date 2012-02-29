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
package com.taobao.metamorphosis.client.consumer.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;


public abstract class BaseOffsetStorageUnitTest {

    protected OffsetStorage offsetStorage;


    @Test
    public void testInitLoadCommitLoad() {
        final String topic = "test";
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        this.offsetStorage.initOffset(topic, group, partition, 0);

        final TopicPartitionRegInfo info = this.offsetStorage.load(topic, group, partition);
        assertEquals(topic, info.getTopic());
        assertEquals(partition, info.getPartition());
        assertEquals(0, info.getOffset().get());
        assertEquals(-1, info.getMessageId());

        info.getOffset().set(999);
        info.setMessageId(100);
        info.setModified(true);
        final Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        infoList.add(info);
        this.offsetStorage.commitOffset(group, infoList);

        final TopicPartitionRegInfo newInfo = this.offsetStorage.load(topic, group, partition);
        assertEquals(topic, newInfo.getTopic());
        assertEquals(partition, newInfo.getPartition());
        assertEquals(999, newInfo.getOffset().get());
        assertEquals(100, info.getMessageId());
    }


    @Test
    public void initDuplicate() {
        final String topic = "test";
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        this.offsetStorage.initOffset(topic, group, partition, 0);
        this.offsetStorage.initOffset(topic, group, partition, 0);
    }


    @Test
    public void testInitCommitMany() {
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        this.offsetStorage.initOffset("test1", group, partition, 0);
        this.offsetStorage.initOffset("test2", group, partition, 0);
        this.offsetStorage.initOffset("test3", group, partition, 0);
        final Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            final TopicPartitionRegInfo info = this.offsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(0, info.getOffset().get());
            info.getOffset().set(i);
            info.setMessageId(i);
            info.setModified(true);
            infoList.add(info);
        }
        this.offsetStorage.commitOffset(group, infoList);
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            final TopicPartitionRegInfo info = this.offsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            assertEquals(i, info.getMessageId());
            info.getOffset().set(i);
            infoList.add(info);
        }
    }


    @Test
    public void testCommitNotExists_NoError() {
        final String topic = "test";
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        final TopicPartitionRegInfo info = new TopicPartitionRegInfo(topic, partition, 1999);
        final Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        infoList.add(info);
        assertNull(this.offsetStorage.load(topic, group, partition));
        this.offsetStorage.commitOffset(group, infoList);
        assertNull(this.offsetStorage.load(topic, group, partition));
    }

}