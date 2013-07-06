package com.taobao.meta.test.ha;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.taobao.meta.test.BaseMetaTest;
import com.taobao.meta.test.Utils;
import com.taobao.metamorphosis.EnhancedBroker;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.SimpleFetchManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-7-12 ÉÏÎç10:38:17
 */
@Ignore
public class HABaseMetaTest extends BaseMetaTest {

    // private final List<EnhancedBroker> slaveBrokers = new
    // ArrayList<EnhancedBroker>();

    @Before
    @Override
    public void setUp() throws Exception {
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        super.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        this.log.info("before run");
    }


    protected EnhancedBroker startSlaveServers(String name, boolean isClearConsumerInfo, boolean isClearMsg)
            throws Exception {
        MetaConfig metaConfig = this.metaConfig(name);
        metaConfig.setDashboardHttpPort(metaConfig.getServerPort() - 20);
        Map<String, Properties> pluginsInfo = this.getSlaveProperties(name);
        if (isClearMsg) {
            Utils.clearDataDir(metaConfig);
        }
        SimpleFetchManager.setMessageIdCache(null);
        EnhancedBroker broker = new EnhancedBroker(metaConfig, pluginsInfo);
        if (isClearConsumerInfo) {
            Utils.clearConsumerInfoInZk(broker.getBroker().getBrokerZooKeeper().getZkClient(), broker.getBroker()
                .getBrokerZooKeeper().getMetaZookeeper());
        }
        broker.start();
        // this.slaveBrokers.add(broker);
        return broker;
    }


    private Map<String, Properties> getSlaveProperties(String name) throws IOException {
        Map<String, Properties> ret = new HashMap<String, Properties>();
        Properties properties =
                com.taobao.metamorphosis.utils.Utils.getResourceAsProperties("async_" + name + ".properties", "GBK");
        ret.put("metaslave", properties);
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
    }

}
