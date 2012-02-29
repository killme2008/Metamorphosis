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

import org.junit.Assert;
import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-22 ÏÂÎç04:28:58
 */

public class BrokerUnitTest {

    @Test
    public void testNewBroker() {
        Broker broker = new Broker(1, "10.2.2.2", 8123);
        Assert.assertTrue(broker.getId() == 1);
        Assert.assertTrue(broker.getHost().equals("10.2.2.2"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == false);
        Assert.assertTrue(broker.getZKString().equals("meta://10.2.2.2:8123"));

        broker = new Broker(1, "10.2.2.2", 8123, 0);
        Assert.assertTrue(broker.getId() == 1);
        Assert.assertTrue(broker.getHost().equals("10.2.2.2"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == true);
        Assert.assertTrue(broker.getSlaveId() == 0);
        Assert.assertTrue(broker.getZKString().equals("meta://10.2.2.2:8123"));

        broker = new Broker(0, "meta://10.2.2.2:8123?slaveId=0");
        Assert.assertTrue(broker.getId() == 0);
        Assert.assertTrue(broker.getHost().equals("10.2.2.2"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == true);
        Assert.assertTrue(broker.getZKString().equals("meta://10.2.2.2:8123"));

        broker = new Broker(0, "meta://10.2.2.2:8123?slaveId=0&xx=yy");
        Assert.assertTrue(broker.getId() == 0);
        Assert.assertTrue(broker.getHost().equals("10.2.2.2"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == true);
        Assert.assertTrue(broker.getSlaveId() == 0);
        Assert.assertTrue(broker.getZKString().equals("meta://10.2.2.2:8123"));

        broker = new Broker(0, "meta://10.2.2.2:8123");
        Assert.assertTrue(broker.getId() == 0);
        Assert.assertTrue(broker.getHost().equals("10.2.2.2"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == false);
        Assert.assertTrue(broker.getZKString().equals("meta://10.2.2.2:8123"));

        broker = new Broker(1, "meta://10.2.2.2:8123?xx=yy");
        Assert.assertTrue(broker.getId() == 1);
        Assert.assertTrue(broker.getHost().equals("10.2.2.2"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == false);
        Assert.assertTrue(broker.getZKString().equals("meta://10.2.2.2:8123"));

    }


    @Test
    public void testIPv6() {
        Broker broker = new Broker(1, "0:0:0:0:0:0:0:1", 8123);
        Assert.assertEquals("meta://[0:0:0:0:0:0:0:1]:8123", broker.getZKString());

        broker = new Broker(1, "meta://[0:0:0:0:0:0:0:1]:8123");
        Assert.assertTrue(broker.getId() == 1);
        Assert.assertTrue(broker.getHost().equals("[0:0:0:0:0:0:0:1]"));
        Assert.assertTrue(broker.getPort() == 8123);
        Assert.assertTrue(broker.isSlave() == false);
        Assert.assertTrue(broker.getZKString().equals("meta://[0:0:0:0:0:0:0:1]:8123"));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNewBroker_invalid() {
        new Broker(1, "meta://10.2.2.2:8123?xx=yy,meta://10.2.2.3:8123?xx=yy");
    }


    @Test
    public void testEquals() {
        Assert.assertTrue(new Broker(1, "meta://10.2.2.2:8123").equals(new Broker(1, "10.2.2.2", 8123)));
        Assert.assertTrue(new Broker(1, "meta://10.2.2.2:8123").equals(new Broker(1, "10.2.2.2", 8123, -1)));
        Assert.assertFalse(new Broker(1, "meta://10.2.2.2:8123?slaveId=1").equals(new Broker(1, "10.2.2.2", 8123)));
    }

}