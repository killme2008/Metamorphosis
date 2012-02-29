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
package com.taobao.metamorphosis.tools.shell;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.tools.query.Query;
import com.taobao.metamorphosis.utils.ZkUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-8-24 ÏÂÎç3:21:59
 */

public class CopyOffsetInZkTest {
    CopyOffsetInZk copyOffsetInZk;
    ZkClient zkClient;
    String topic = "CopyOffsetTopic";
    String groupPrefix = "CopyOffsetGroup";
    Query query;


    @Before
    public void setUp() throws Exception {
        this.copyOffsetInZk = new CopyOffsetInZk(System.out);
        this.query = this.copyOffsetInZk.getQuery();
        this.zkClient = query.getZkClient();
        for (int i = 1; i <= 2; i++) {
            ZkUtils.updatePersistentPath(this.zkClient,
                query.getOffsetPath(this.groupPrefix + i, this.topic, new Partition(1, 4)), "44" + i);
            ZkUtils.updatePersistentPath(this.zkClient,
                query.getOffsetPath(this.groupPrefix + i, this.topic, new Partition(1, 5)), "55" + i);
            ZkUtils.updatePersistentPath(this.zkClient,
                query.getOffsetPath(this.groupPrefix + i, this.topic, new Partition(1, 6)), "66" + i);
            ZkUtils.updatePersistentPath(this.zkClient,
                query.getOffsetPath(this.groupPrefix + i, this.topic, new Partition(1, 7)), "77" + i);
        }

    }


    @Test
    public void testDoMain() throws Exception {

        this.copyOffsetInZk.doMain(("-topic " + this.topic + " -src 1 -target 2 -start 6 -end 7 ").split(" "));

        long offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 1, this.topic, new Partition(2, 0))));
        Assert.assertEquals(661, offset);
        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 1, this.topic, new Partition(2, 1))));
        Assert.assertEquals(771, offset);

        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 2, this.topic, new Partition(2, 0))));
        Assert.assertEquals(662, offset);

        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 2, this.topic, new Partition(2, 1))));
        Assert.assertEquals(772, offset);

    }


    @Test
    public void testDoMain2() throws Exception {
        this.copyOffsetInZk.doMain(("-topic " + this.topic + " -src 1 -target 2 -start 6 -end 7 -targetStart 2")
            .split(" "));

        long offset;
        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 1, this.topic, new Partition(2, 2))));
        Assert.assertEquals(661, offset);
        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 1, this.topic, new Partition(2, 3))));
        Assert.assertEquals(771, offset);

        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 2, this.topic, new Partition(2, 2))));
        Assert.assertEquals(662, offset);

        offset =
                Long.parseLong(ZkUtils.readData(this.zkClient,
                    query.getOffsetPath(this.groupPrefix + 2, this.topic, new Partition(2, 3))));
        Assert.assertEquals(772, offset);
    }


    @After
    public void tearDown() throws Exception {
        ZkUtils.deletePathRecursive(this.zkClient, "/meta/consumers/CopyOffsetGroup1");
        ZkUtils.deletePathRecursive(this.zkClient, "/meta/consumers/CopyOffsetGroup2");
    }
}