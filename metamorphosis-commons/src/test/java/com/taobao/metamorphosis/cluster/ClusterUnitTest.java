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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


/**
 * 
 * @author 无花
 * @since 2011-6-23 下午06:48:32
 */

public class ClusterUnitTest {

    @Test
    public void testEquals() {
        Cluster cluster1 = new Cluster();
        Cluster cluster2 = null;
        Assert.assertFalse(cluster1.equals(cluster2));

        cluster1 = new Cluster();
        cluster1.addBroker(1, new Broker(1, "meta://host:8123"));
        cluster1.addBroker(1, new Broker(1, "meta://host1:8123?slaveId=1"));
        cluster2 = new Cluster();
        cluster2.addBroker(1, new Broker(1, "meta://host:8123"));
        Assert.assertFalse(cluster1.equals(cluster2));
        cluster2.addBroker(1, new Broker(1, "meta://host1:8123?slaveId=1"));
        Assert.assertTrue(cluster1.equals(cluster2));

        cluster1 = new Cluster();
        cluster1.addBroker(1, new Broker(1, "meta://host:8123"));
        cluster1.addBroker(1, new Broker(1, "meta://host1:8123?slaveId=1"));
        cluster2 = new Cluster();
        cluster2.addBroker(1, new Broker(1, "meta://host:8123"));
        cluster2.addBroker(1, new Broker(1, "meta://host1:8123"));
        Assert.assertFalse(cluster1.equals(cluster2));
    }


    @Ignore
    public void testGetBrokerRandom() {
        Cluster cluster1 = new Cluster();
        cluster1.addBroker(1, new Broker(1, "meta://host:8123"));
        cluster1.addBroker(1, new Broker(1, "meta://host1:8123?slaveId=1"));

        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            list.add(cluster1.getBrokerRandom(1).getZKString());
        }
        // 10次总能取到两个不同的吧
        Assert.assertTrue(list.contains("meta://host:8123"));
        Assert.assertTrue(list.contains("meta://host1:8123"));

    }


    @Test
    public void testSize() {
        Cluster cluster1 = new Cluster();
        Assert.assertEquals(0, cluster1.size());
        cluster1.addBroker(1, new Broker(1, "meta://host:8123"));
        Assert.assertEquals(1, cluster1.size());
        cluster1.addBroker(1, new Broker(1, "meta://host1:8123?isSlave=true"));
        Assert.assertEquals(2, cluster1.size());
        cluster1.addBroker(2, new Broker(2, "meta://host2:8123"));
        Assert.assertEquals(3, cluster1.size());

        cluster1.remove(1);
        Assert.assertEquals(1, cluster1.size());
        Set<Broker> brokers = new HashSet<Broker>();
        cluster1.addBroker(4, brokers);
        Assert.assertEquals(1, cluster1.size());
        //两个broker是相同的,相当于一个
        brokers.add(new Broker(4, "meta://host4:8123"));
        brokers.add(new Broker(4, "meta://host4:8123"));
        cluster1.addBroker(4, brokers);
        Assert.assertEquals(2, cluster1.size());
        brokers.add(new Broker(4, "meta://host44:8123?isSlave=true"));
        cluster1.addBroker(4, brokers);
        Assert.assertEquals(3, cluster1.size());

        Assert.assertFalse(cluster1.getMasterBroker(4).isSlave());

    }

}