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
package com.taobao.metamorphosis.server.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.utils.ResourceUtils;


/**
 * 
 * @author ÎÞ»¨,dennis
 * @since 2011-6-22 ÏÂÎç03:21:49
 */

public class MetaConfigUnitTest {

    @Test
    public void testIsSlave() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.setSlaveConfig(new SlaveConfig(1));
        Assert.assertTrue(metaConfig.isSlave());

        metaConfig = new MetaConfig();
        metaConfig.setSlaveConfig(new SlaveConfig(-2));
        Assert.assertFalse(metaConfig.isSlave());

        Assert.assertFalse(new MetaConfig().isSlave());
    }


    @Test
    public void testAccpetPublishSubscribe() throws Exception {
        MetaConfig metaConfig = new MetaConfig();
        this.LoadProperty(metaConfig, "master_noBrokerId.ini");

        assertTrue(metaConfig.isAcceptPublish());
        assertFalse(metaConfig.isAcceptSubscribe());

        TopicConfig test1Config = metaConfig.getTopicConfig("test1");
        assertTrue(test1Config.isAcceptPublish());
        assertFalse(test1Config.isAcceptSubscribe());

        TopicConfig test2Config = metaConfig.getTopicConfig("test2");
        assertTrue(test2Config.isAcceptSubscribe());
        assertFalse(test2Config.isAcceptPublish());
    }

    @Test
    public void testIsSlave_LoadProperty() throws Exception {
        MetaConfig metaConfig = new MetaConfig();
        this.LoadProperty(metaConfig, "master_brokerIdEmpty.ini");
        Assert.assertEquals(-1, metaConfig.getSlaveId());
        Assert.assertFalse(metaConfig.isSlave());
        assertEquals("com.avos.TestFilter1", metaConfig.getTopicConfig("test1").getFilterClass("A"));
        assertEquals("com.avos.TestFilter2", metaConfig.getTopicConfig("test1").getFilterClass("B"));
        assertNull(metaConfig.getTopicConfig("test2").getFilterClass("B"));
        assertNull(metaConfig.getTopicConfig("test2").getFilterClass("A"));

        metaConfig = new MetaConfig();
        this.LoadProperty(metaConfig, "master_noBrokerId.ini");
        Assert.assertEquals(-1, metaConfig.getSlaveId());
        Assert.assertFalse(metaConfig.isSlave());

        // metaConfig = new MetaConfig();
        // this.LoadProperty(metaConfig, "slave.ini");
        // Assert.assertEquals(0, metaConfig.getSlaveId());
        // Assert.assertTrue(metaConfig.isSlave());
    }


    private void LoadProperty(MetaConfig metaConfig, String fileName) throws IOException {
        metaConfig.loadFromFile(ResourceUtils.getResourceAsFile(
            this.getClass().getPackage().getName().replaceAll("\\.", "/") + "/" + fileName).getAbsolutePath());
    }


    @Test
    public void testClosePartitions_topicNotPublished() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.closePartitions("topic1", 1, 2);
        assertFalse(metaConfig.isClosedPartition("topic1", 1));
    }


    @Test
    public void testClosePartitions() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.setTopics(Arrays.asList("topic1"));
        metaConfig.closePartitions("topic1", 1, 2);
        assertTrue(metaConfig.isClosedPartition("topic1", 1));
    }
}