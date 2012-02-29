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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.ResourceUtils;


public class LocalOffsetStorageUnitTest {

    private OffsetStorage offsetStorage;
    private File file;


    @Before
    public void setUp() throws Exception {
        this.file = new File(System.getProperty("user.home") + File.separator + ".meta_offsets");
        if (this.file.exists()) {
            this.file.delete();
        }
        this.offsetStorage = new LocalOffsetStorage();
    }


    @After
    public void tearDown() throws Exception {
        this.offsetStorage.close();
    }


    @Test
    public void testCommitLoad() throws Exception {
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            infoList.add(new TopicPartitionRegInfo(topic, partition, i));
        }
        this.offsetStorage.commitOffset(group, infoList);
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            TopicPartitionRegInfo info = this.offsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }
        OffsetStorage newOffsetStorage = new LocalOffsetStorage();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            TopicPartitionRegInfo info = newOffsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }
        newOffsetStorage.close();

        assertTrue(this.file.exists());
        String content = this.readFile(this.file);
        assertFalse(content.contains("autoAck"));
        assertFalse(content.contains("acked"));
        assertFalse(content.contains("rollback"));

    }


    @Test
    public void testCommitCloseLoad() throws Exception {
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            infoList.add(new TopicPartitionRegInfo(topic, partition, i));
        }
        this.offsetStorage.commitOffset(group, infoList);

        this.offsetStorage.close();
        this.offsetStorage = new LocalOffsetStorage();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            TopicPartitionRegInfo info = this.offsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }
    }


    @Test
    public void testCommitLoadEmpty() throws Exception {
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            infoList.add(new TopicPartitionRegInfo(topic, partition, i));
        }
        this.offsetStorage.commitOffset(group, infoList);
        this.offsetStorage.commitOffset(group, null);
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            TopicPartitionRegInfo info = this.offsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }
        OffsetStorage newOffsetStorage = new LocalOffsetStorage();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            TopicPartitionRegInfo info = newOffsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }

        this.offsetStorage.commitOffset(group, Collections.EMPTY_LIST);
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            TopicPartitionRegInfo info = newOffsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }
        newOffsetStorage.close();

    }


    @Test
    public void testBackwardCompatibility() throws IOException {
        OffsetStorage offsetStorage =
                new LocalOffsetStorage(ResourceUtils.getResourceAsFile("oldVersion_meta_offsets").getAbsolutePath());
        final String group = "test-grp";
        final String topic = "test-topic";
        Partition partition1 = new Partition("100-0");
        Partition partition2 = new Partition("101-0");
        TopicPartitionRegInfo info = offsetStorage.load(topic, group, partition1);
        assertEquals(topic, info.getTopic());
        assertEquals(partition1, info.getPartition());
        assertEquals(0, info.getOffset().get());
        // 以下这几个值不管文件中有没有,都是按照默认的
        assertEquals(true, info.getPartition().isAutoAck());
        assertEquals(true, info.getPartition().isAcked());
        assertEquals(false, info.getPartition().isRollback());

        TopicPartitionRegInfo info2 = offsetStorage.load(topic, group, partition2);
        assertEquals(topic, info2.getTopic());
        assertEquals(partition2, info2.getPartition());
        assertEquals(130835445, info2.getOffset().get());
        assertEquals(true, info2.getPartition().isAutoAck());
        assertEquals(true, info2.getPartition().isAcked());
        assertEquals(false, info2.getPartition().isRollback());

    }


    private String readFile(File file) throws Exception {
        String line = null;
        BufferedReader reader = null;
        FileReader fileReader = null;
        StringBuilder jsonSB = new StringBuilder();
        try {
            fileReader = new FileReader(file);
            reader = new BufferedReader(fileReader);
            while ((line = reader.readLine()) != null) {
                jsonSB.append(line);
            }
        }
        finally {
            reader.close();
            fileReader.close();
        }
        return jsonSB.toString();

    }
}