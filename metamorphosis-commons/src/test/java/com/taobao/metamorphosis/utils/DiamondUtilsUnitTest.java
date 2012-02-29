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
package com.taobao.metamorphosis.utils;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-8-3 ÉÏÎç11:30:37
 */

public class DiamondUtilsUnitTest {

    @Test
    public void testParsePartitions() {
        Assert.assertTrue(DiamondUtils.parsePartitions(null) != null);
        Assert.assertTrue(DiamondUtils.parsePartitions(null).size() == 0);
        Assert.assertTrue(DiamondUtils.parsePartitions("") != null);
        Assert.assertTrue(DiamondUtils.parsePartitions("").size() == 0);
        List<Partition> partitions = DiamondUtils.parsePartitions("1:3");
        Assert.assertTrue(partitions.size() == 3);
        this.validateOrder(partitions);

        partitions = DiamondUtils.parsePartitions("5:4;1:3");
        Assert.assertTrue(partitions.size() == 7);
        this.validateOrder(partitions);

        partitions = DiamondUtils.parsePartitions(";5:4;;1:3;;");
        Assert.assertTrue(partitions.size() == 7);
        this.validateOrder(partitions);

        partitions = DiamondUtils.parsePartitions("5:4;1:3;2:2");
        Assert.assertTrue(partitions.size() == 9);
        this.validateOrder(partitions);

    }


    @Test(expected = NumberFormatException.class)
    public void testParsePartitions_error() {
        DiamondUtils.parsePartitions("5:4;1:0;2:2");
    }


    @Test(expected = NumberFormatException.class)
    public void testParsePartitions_error2() {
        DiamondUtils.parsePartitions("5:4;1;2:2");
    }


    private void validateOrder(List<Partition> partitions) {
        System.out.println(partitions);
        for (int i = 0; i < partitions.size() - 1; i++) {
            Assert.assertTrue(partitions.get(i).compareTo(partitions.get(i + 1)) == -1);
        }
    }
}