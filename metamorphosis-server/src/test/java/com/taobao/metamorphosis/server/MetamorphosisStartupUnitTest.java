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
package com.taobao.metamorphosis.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import org.junit.Test;

import com.taobao.gecko.core.util.ResourcesUtils;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;


public class MetamorphosisStartupUnitTest {

    @Test(expected = MetamorphosisServerStartupException.class)
    public void testGetConfigFilePathOneArgs() {
        final String[] args = { "-f" };
        MetamorphosisStartup.getConfigFilePath(args);
    }


    @Test(expected = MetamorphosisServerStartupException.class)
    public void testGetConfigFilePathBlankArgs() {
        final String[] args = { "-f", "" };
        MetamorphosisStartup.getConfigFilePath(args);
    }


    @Test
    public void testGetConfigFilePath() {
        final String[] args = { "-f", "server.test" };
        assertEquals("server.test", MetamorphosisStartup.getConfigFilePath(args));
    }


    @Test
    public void testGetMetaConfig() throws Exception {
        final File file = ResourcesUtils.getResourceAsFile("server.ini");
        final MetaConfig config = MetamorphosisStartup.getMetaConfig(file.getAbsolutePath());
        assertEquals(1111, config.getBrokerId());
        assertEquals("test.localhost", config.getHostName());
        assertEquals(10, config.getNumPartitions());
        assertEquals(8124, config.getServerPort());
        assertEquals("/home/admin", config.getDataPath());
        assertEquals("/home/datalog", config.getDataLogPath());
        assertEquals(10000, config.getUnflushThreshold());
        assertEquals(100000, config.getUnflushInterval());
        assertEquals(1024 * 1024 * 1024, config.getMaxSegmentSize());
        assertEquals(1024 * 1024, config.getMaxTransferSize());
        assertEquals(90, config.getGetProcessThreadCount());
        assertEquals(90, config.getPutProcessThreadCount());
        assertEquals(2, config.getTopics().size());
        assertTrue(config.getTopics().contains("test1"));
        assertTrue(config.getTopics().contains("test2"));

        final Map<String, TopicConfig> topicConfigs = config.getTopicConfigMap();
        assertEquals(2, topicConfigs.size());
        assertEquals(11, topicConfigs.get("test1").getNumPartitions());
        assertEquals(13, topicConfigs.get("test2").getNumPartitions());

        assertEquals("delete,77", config.getTopicConfig("test1").getDeletePolicy());

        assertEquals("127.0.0.1:2181", config.getZkConfig().zkConnect);
        assertEquals(30000, config.getZkConfig().zkSessionTimeoutMs);
        assertEquals(40000, config.getZkConfig().zkConnectionTimeoutMs);
        assertEquals(5000, config.getZkConfig().zkSyncTimeMs);

        assertEquals("delete,999", config.getDeletePolicy());

        final TopicConfig topicConfig1 = config.getTopicConfig("test1");
        final TopicConfig topicConfig2 = config.getTopicConfig("test2");
        assertEquals("/home/admin", topicConfig1.getDataPath());
        assertEquals("/test2", topicConfig2.getDataPath());
        assertFalse(topicConfig1.isStat());
        assertTrue(topicConfig2.isStat());
    }
}