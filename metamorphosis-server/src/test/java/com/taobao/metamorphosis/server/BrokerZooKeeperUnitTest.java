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
import static org.junit.Assert.fail;

import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.SlaveConfig;
import com.taobao.metamorphosis.utils.JSONUtils;
import com.taobao.metamorphosis.utils.ZkUtils;


public class BrokerZooKeeperUnitTest {
    private ZkClient client;
    private BrokerZooKeeper brokerZooKeeper;
    private BrokerZooKeeper slaveBrokerZooKeeper;


    @Before
    public void setUp() {
        MetaConfig metaConfig = new MetaConfig();
        this.brokerZooKeeper = new BrokerZooKeeper(metaConfig);
        this.client = this.brokerZooKeeper.getZkClient();
    }


    @Test
    public void testRegisterBrokerInZk_master() throws Exception {
        String path = "/meta/brokers/ids/0/master";
        assertFalse(ZkUtils.pathExists(this.client, path));
        this.brokerZooKeeper.registerBrokerInZk();
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertEquals("meta://" + RemotingUtils.getLocalHost() + ":8123", ZkUtils.readData(this.client, path));
        // register twice
        try {
            this.brokerZooKeeper.registerBrokerInZk();
            fail();
        }
        catch (ZkNodeExistsException e) {

        }
    }


    @Test
    public void testRegisterMasterConfigFileChecksumInZk() throws Exception {
        String path = "/meta/brokers/ids/0/master_config_checksum";
        assertFalse(ZkUtils.pathExists(this.client, path));
        this.brokerZooKeeper.registerMasterConfigFileChecksumInZk();
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertEquals(String.valueOf(this.brokerZooKeeper.getConfig().getConfigFileChecksum()),
            ZkUtils.readData(this.client, path));
        // register twice
        try {
            this.brokerZooKeeper.registerMasterConfigFileChecksumInZk();
            fail();
        }
        catch (ZkNodeExistsException e) {

        }
    }


    @Test
    public void testConfigFileChecksumChanged() throws Exception {
        String path = "/meta/brokers/ids/0/master_config_checksum";
        this.brokerZooKeeper.registerMasterConfigFileChecksumInZk();
        long old = this.brokerZooKeeper.getConfig().getConfigFileChecksum();
        assertEquals(String.valueOf(old), ZkUtils.readData(this.client, path));
        // set a new value
        int newValue = 9999;
        this.brokerZooKeeper.getConfig().setConfigFileChecksum(newValue);
        Thread.sleep(1000);
        assertEquals(String.valueOf(newValue), ZkUtils.readData(this.client, path));
        assertFalse(newValue == old);
    }


    @Test
    public void testRegisterBrokerInZk_slave() throws Exception {
        String path = "/meta/brokers/ids/0/slave0";
        this.createSlaveBrokerZooKeeper();

        assertFalse(ZkUtils.pathExists(this.slaveBrokerZooKeeper.getZkClient(), path));
        this.slaveBrokerZooKeeper.registerBrokerInZk();
        assertTrue(ZkUtils.pathExists(this.slaveBrokerZooKeeper.getZkClient(), path));
        assertEquals("meta://" + RemotingUtils.getLocalHost() + ":8123",
            ZkUtils.readData(this.slaveBrokerZooKeeper.getZkClient(), path));
        // register twice
        try {
            this.slaveBrokerZooKeeper.registerBrokerInZk();
            fail();
        }
        catch (ZkNodeExistsException e) {

        }
    }


    @Test
    public void testRegisterTopicInZk() throws Exception {
        final String topic = "test";
        final String path = "/meta/brokers/topics/test/0-m";
        final String pubPath = "/meta/brokers/topics-pub/test/0-m";
        final String subPath = "/meta/brokers/topics-sub/test/0-m";
        assertFalse(ZkUtils.pathExists(this.client, path));
        assertFalse(ZkUtils.pathExists(this.client, pubPath));
        assertFalse(ZkUtils.pathExists(this.client, subPath));
        this.brokerZooKeeper.registerTopicInZk(topic, false);
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertTrue(ZkUtils.pathExists(this.client, pubPath));
        assertTrue(ZkUtils.pathExists(this.client, subPath));
        assertEquals("1", ZkUtils.readData(this.client, path));

        assertEquals(this.deserializeMap("{\"numParts\":1,\"broker\":\"0-m\"}"),
            JSONUtils.deserializeObject(ZkUtils.readData(this.client, pubPath), Map.class));
        assertEquals(this.deserializeMap("{\"numParts\":1,\"broker\":\"0-m\"}"),
            JSONUtils.deserializeObject(ZkUtils.readData(this.client, subPath), Map.class));
    }


    private Object deserializeMap(String s) throws Exception {
        return JSONUtils.deserializeObject(s, Map.class);
    }


    @Test
    public void testRegisterTopicInZk_slave() throws Exception {
        final String topic = "test";
        final String path = "/meta/brokers/topics/test/0-s0";
        final String pubPath = "/meta/brokers/topics-pub/test/0-s0";
        final String subPath = "/meta/brokers/topics-sub/test/0-s0";
        assertFalse(ZkUtils.pathExists(this.client, path));
        assertFalse(ZkUtils.pathExists(this.client, pubPath));
        assertFalse(ZkUtils.pathExists(this.client, subPath));
        this.createSlaveBrokerZooKeeper();
        this.slaveBrokerZooKeeper.registerTopicInZk(topic, false);
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertTrue(ZkUtils.pathExists(this.client, pubPath));
        assertTrue(ZkUtils.pathExists(this.client, subPath));
        assertEquals("1", ZkUtils.readData(this.client, path));
        assertEquals(this.deserializeMap("{\"numParts\":1,\"broker\":\"0-s0\"}"),
            this.deserializeMap(ZkUtils.readData(this.client, pubPath)));
        assertEquals(this.deserializeMap("{\"numParts\":1,\"broker\":\"0-s0\"}"),
            this.deserializeMap(ZkUtils.readData(this.client, subPath)));

    }


    @Test
    public void testMasterClose_slaveNotClose() throws Exception {
        String masterBrokerPath = "/meta/brokers/ids/0/master";
        String masterTopicPath = "/meta/brokers/topics/test/0-m";
        String slaveBrokerPath = "/meta/brokers/ids/0/slave0";
        String slaveTopicPath = "/meta/brokers/topics/test/0-s0";
        this.testRegisterBrokerInZk_master();
        this.testRegisterTopicInZk();
        this.testRegisterBrokerInZk_slave();
        this.testRegisterTopicInZk_slave();
        this.brokerZooKeeper.close(true);
        this.brokerZooKeeper = null;

        // master注册信息不存在
        assertFalse(ZkUtils.pathExists(this.slaveBrokerZooKeeper.getZkClient(), masterBrokerPath));
        assertFalse(ZkUtils.pathExists(this.slaveBrokerZooKeeper.getZkClient(), masterTopicPath));

        // slave注册信息还存在
        assertTrue(ZkUtils.pathExists(this.slaveBrokerZooKeeper.getZkClient(), slaveBrokerPath));
        assertEquals("meta://" + RemotingUtils.getLocalHost() + ":8123",
            ZkUtils.readData(this.slaveBrokerZooKeeper.getZkClient(), slaveBrokerPath));
        assertTrue(ZkUtils.pathExists(this.slaveBrokerZooKeeper.getZkClient(), slaveTopicPath));
        assertEquals("1", ZkUtils.readData(this.slaveBrokerZooKeeper.getZkClient(), slaveTopicPath));
    }


    @Test
    public void testSlaveClose_masterNotClose() throws Exception {
        String masterBrokerPath = "/meta/brokers/ids/0/master";
        String masterTopicPath = "/meta/brokers/topics/test/0-m";
        String slaveBrokerPath = "/meta/brokers/ids/0/slave0";
        String slaveTopicPath = "/meta/brokers/topics/test/0-s0";
        this.testRegisterBrokerInZk_master();
        this.testRegisterTopicInZk();
        this.testRegisterBrokerInZk_slave();
        this.testRegisterTopicInZk_slave();
        this.slaveBrokerZooKeeper.close(true);
        this.slaveBrokerZooKeeper = null;

        // slave注册信息不存在
        assertFalse(ZkUtils.pathExists(this.brokerZooKeeper.getZkClient(), slaveBrokerPath));
        assertFalse(ZkUtils.pathExists(this.brokerZooKeeper.getZkClient(), slaveTopicPath));

        // master注册信息还存在
        assertTrue(ZkUtils.pathExists(this.brokerZooKeeper.getZkClient(), masterBrokerPath));
        assertEquals("meta://" + RemotingUtils.getLocalHost() + ":8123",
            ZkUtils.readData(this.brokerZooKeeper.getZkClient(), masterBrokerPath));
        assertTrue(ZkUtils.pathExists(this.brokerZooKeeper.getZkClient(), masterTopicPath));
        assertEquals("1", ZkUtils.readData(this.brokerZooKeeper.getZkClient(), masterTopicPath));
    }


    @Test
    public void testReRegisterEverthing() throws Exception {
        this.testRegisterBrokerInZk_master();
        this.testRegisterTopicInZk();

        final String brokerPath = "/meta/brokers/ids/0/master";
        final String topicPath = "/meta/brokers/topics/test/0-m";
        final String topicPubPath = "/meta/brokers/topics-pub/test/0-m";
        final String topicSubPath = "/meta/brokers/topics-sub/test/0-m";

        assertTrue(ZkUtils.pathExists(this.client, brokerPath));
        assertTrue(ZkUtils.pathExists(this.client, topicPath));
        assertTrue(ZkUtils.pathExists(this.client, topicPubPath));
        assertTrue(ZkUtils.pathExists(this.client, topicSubPath));

        ZkUtils.deletePath(this.client, brokerPath);
        ZkUtils.deletePath(this.client, topicPath);
        ZkUtils.deletePath(this.client, topicPubPath);
        ZkUtils.deletePath(this.client, topicSubPath);

        assertFalse(ZkUtils.pathExists(this.client, brokerPath));
        assertFalse(ZkUtils.pathExists(this.client, topicPath));
        assertFalse(ZkUtils.pathExists(this.client, topicPubPath));
        assertFalse(ZkUtils.pathExists(this.client, topicSubPath));

        this.brokerZooKeeper.reRegisterEveryThing();
        assertTrue(ZkUtils.pathExists(this.client, brokerPath));
        assertTrue(ZkUtils.pathExists(this.client, topicPath));
        assertTrue(ZkUtils.pathExists(this.client, topicPubPath));
        assertTrue(ZkUtils.pathExists(this.client, topicSubPath));

    }


    private BrokerZooKeeper createSlaveBrokerZooKeeper() {
        MetaConfig slaveMetaConfig = new MetaConfig();
        slaveMetaConfig.setSlaveConfig(new SlaveConfig(0));
        this.slaveBrokerZooKeeper = new BrokerZooKeeper(slaveMetaConfig);
        return this.slaveBrokerZooKeeper;
    }


    private void closeBrokerZooKeeper(BrokerZooKeeper brokerZooKeeper) {
        if (brokerZooKeeper == null) {
            return;
        }
        brokerZooKeeper.close(true);
    }


    @After
    public void tearDown() throws Exception {
        this.closeBrokerZooKeeper(this.brokerZooKeeper);
        this.closeBrokerZooKeeper(this.slaveBrokerZooKeeper);
    }

}