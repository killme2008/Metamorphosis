package com.taobao.meta.test.gregor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.meta.test.BaseMetaTest;
import com.taobao.meta.test.Utils;
import com.taobao.metamorphosis.EnhancedBroker;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-29
 * 
 */
public class GregorMasterSlaveTest extends BaseMetaTest {

    // private final List<EnhancedBroker> slaveBrokers = new
    // ArrayList<EnhancedBroker>();
    private final String topic = "meta-test";
    private EnhancedBroker master, slave;


    @Before
    @Override
    public void setUp() throws Exception {
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        super.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        this.log.info("before run");
    }


    @Test
    public void testOneMasterOneSlaveOneProducerOneConsumer() throws Exception {
        // start slave
        this.slave = this.startEnhanceBroker("gregor_server1", true, true, this.getSlaveProperties());
        // start master
        this.master = this.startEnhanceBroker("samsa_server1", true, true, this.getMasterProperties());
        try {
            this.createProducer();
            this.producer.publish(this.topic);
            final byte[] data = "testOneMasterOneSlaveOneProducerOneConsumer".getBytes();
            this.sendMessage(100, data, this.topic);
            final String group = "GregorMasterSlaveTest";
            this.createConsumer(group);
            this.subscribe(this.topic, 1024 * 1024, 100);
        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }
    }


    @Test
    public void testOneMasterOneSlaveNProducerNConsumer() throws Exception {
        // start slave
        this.slave = this.startEnhanceBroker("gregor_server1", true, true, this.getSlaveProperties());
        // start master
        this.master = this.startEnhanceBroker("samsa_server1", true, true, this.getMasterProperties());
        try {
            this.create_nProducer(10);
            this.sendMessage_nProducer(100, "testOneMasterOneSlaveNProducerNConsumer", this.topic, 10);
            this.subscribe_nConsumer(this.topic, 1024 * 1024, 100, 10, 10);
            // this.subscribe(this.topic, 1024 * 1024, 100);
        }
        finally {
            Utils.shutdown(this.producerList);
            Utils.shutdown(this.consumerList);
        }
    }


    @Test
    public void testSlaveAsMaster() throws Exception {
        // 先发送100条消息到master和slave
        // start slave
        this.testOneMasterOneSlaveOneProducerOneConsumer();
        try {
            this.stopMasterSlave();
            this.queue.clear();
            Thread.sleep(5000);
            // 然后slave作为master启动，master作为slave启动，再发送100条
            this.slave = this.startEnhanceBroker("samsa_server1", false, false, this.getSlaveProperties());
            final Map<String, Properties> masterProperties = this.getMasterProperties();
            // 现在slave是8123端口
            masterProperties.get("samsa").put("slave", "localhost:8123");
            this.master = this.startEnhanceBroker("gregor_server1", false, false, masterProperties);
            // 等待producer和consumer重连
            Thread.sleep(2000);
            final byte[] data = "testSlaveAsMaster".getBytes();
            this.createProducer();
            this.producer.publish(this.topic);
            this.sendMessage(100, data, this.topic);
            final String group = "GregorMasterSlaveTest";
            this.createConsumer(group);
            this.subscribe(this.topic, 1024 * 1024, 100);

        }
        finally {
            Utils.shutdown(this.producer);
            Utils.shutdown(this.consumer);
            Utils.shutdown(this.producerList);
            Utils.shutdown(this.consumerList);
        }
    }


    protected EnhancedBroker startEnhanceBroker(final String name, final boolean isClearConsumerInfo,
            final boolean isClearMsg, final Map<String, Properties> pluginsInfo) throws Exception {
        final MetaConfig metaConfig = this.metaConfig(name);
        metaConfig.setDashboardHttpPort(metaConfig.getServerPort() - 80);
        if (isClearMsg) {
            Utils.clearDataDir(metaConfig);
        }
        final EnhancedBroker broker = new EnhancedBroker(metaConfig, pluginsInfo);
        if (isClearConsumerInfo) {
            Utils.clearConsumerInfoInZk(broker.getBroker().getBrokerZooKeeper().getZkClient(), broker.getBroker()
                .getBrokerZooKeeper().getMetaZookeeper());
        }
        broker.start();
        // this.slaveBrokers.add(broker);
        return broker;
    }


    private Map<String, Properties> getSlaveProperties() throws IOException {
        final Map<String, Properties> ret = new HashMap<String, Properties>();
        final Properties properties =
                com.taobao.metamorphosis.utils.Utils.getResourceAsProperties("gregor_slave.properties", "GBK");
        ret.put("gregor", properties);
        return ret;
    }


    private Map<String, Properties> getMasterProperties() throws IOException {
        final Map<String, Properties> ret = new HashMap<String, Properties>();
        final Properties properties =
                com.taobao.metamorphosis.utils.Utils.getResourceAsProperties("samsa_master.properties", "GBK");
        ret.put("samsa", properties);
        return ret;
    }


    @Override
    @After
    public void tearDown() throws Exception {
        int count = 0;
        count = Utils.shutdown(this.producer);
        this.log.info(count > 0 ? count : "No" + " producer have been shutdown");

        count = Utils.shutdown(this.consumer);
        this.log.info(count > 0 ? count : "No" + " producer have been shutdown");

        count = Utils.shutdown(super.producerList);
        this.log.info(count + " producers have been shutdown");

        count = Utils.shutdown(super.consumerList);
        this.log.info(count + " consumers have been shutdown");

        super.tearDown();
        this.stopMasterSlave();
    }


    private void stopMasterSlave() {
        try {
            this.master.stop();
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
        try {
            this.slave.stop();
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
    }

}
