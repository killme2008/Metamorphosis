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
package com.taobao.metamorphosis.client.extension.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.extension.producer.AvailablePartitionNumException;
import com.taobao.metamorphosis.client.extension.producer.OrderedMessagePartitionSelector;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 
 * @author 无花
 * @since 2011-8-8 下午1:38:02
 */

public class OrderedMessagePartitionSelectorUnitTest {

    private OrderedMessagePartitionSelector selector;
    private final static String testTopic = "topic1";


    @Before
    public void setUp() {
        this.selector = new OrderedMessagePartitionSelector() {
            @Override
            protected Partition choosePartition(String topic, List<Partition> partitions, Message message) {
                return new Partition("0-0");
            }
        };
        Map<String, List<Partition>> map = new HashMap<String, List<Partition>>();
        map.put(testTopic, Arrays.asList(new Partition("0-0"), new Partition("0-1"), new Partition("1-0")));
        this.selector.setConfigPartitions(map);

    }


    @Test
    public void testGetPartition_normal() throws MetaClientException {
        // 正常情况

        Message message = this.createDefaultMessage();
        Partition partition =
                this.selector.getPartition(message.getTopic(),
                    Arrays.asList(new Partition("0-0"), new Partition("0-1"), new Partition("1-0")), message);
        Assert.assertEquals(new Partition("0-0"), partition);
    }


    @Test(expected = MetaClientException.class)
    public void testGetPartition_configPartitionsNull() throws MetaClientException {
        // 分区的配置为null
        this.selector.setConfigPartitions(null);
        Message message = this.createDefaultMessage();
        this.selector.getPartition(message.getTopic(),
            Arrays.asList(new Partition("0-0"), new Partition("0-1"), new Partition("1-0")), message);
    }


    @Test(expected = MetaClientException.class)
    public void testGetPartition_configPartitionsEmpty() throws MetaClientException {
        // 分区的配置为空
        Map<String, List<Partition>> map = new HashMap<String, List<Partition>>();
        map.put(testTopic, new ArrayList<Partition>());
        this.selector.setConfigPartitions(map);
        Message message = this.createDefaultMessage();
        this.selector.getPartition(message.getTopic(),
            Arrays.asList(new Partition("0-0"), new Partition("0-1"), new Partition("1-0")), message);
    }


    @Test(expected = AvailablePartitionNumException.class)
    public void testGetPartition_availablePartitionsNull() throws MetaClientException {
        // 可用分区s为null
        Message message = this.createDefaultMessage();
        this.selector.getPartition(message.getTopic(), null, message);
    }


    @Test(expected = AvailablePartitionNumException.class)
    public void testGetPartition_availablePartitionsEmpty() throws MetaClientException {
        // 可用分区s为空
        Message message = this.createDefaultMessage();
        this.selector.getPartition(message.getTopic(), new ArrayList<Partition>(), message);
    }


    @Test
    public void testGetPartition_availablePartitionsChanged_butSelectedPartitionAvailable() throws MetaClientException {
        // 可用分区发生了变化但总数没变(0-1 -> 1-0),选择出来的分区是0-0，返回这个分区（这个分区继续可写）
        Message message = this.createDefaultMessage();
        Partition partition =
                this.selector.getPartition(message.getTopic(),
                    Arrays.asList(new Partition("0-0"), new Partition("1-0"), new Partition("2-0")), message);
        Assert.assertEquals(new Partition("0-0"), partition);
    }


    @Test(expected = AvailablePartitionNumException.class)
    public void testGetPartition_availablePartitionsChanged_andSelectedPartitionInvalid() throws MetaClientException {
        // 可用分区数量不变,但是选出来的分区（0-0），不包含在可用分区里（可能是用户乱写了不存在的一个分区）
        Message message = this.createDefaultMessage();
        this.selector.getPartition(message.getTopic(),
            Arrays.asList(new Partition("1-0"), new Partition("2-0"), new Partition("3-0")), message);
    }


    @Test(expected = AvailablePartitionNumException.class)
    public void testGetPartition_configPartitionsChanged() throws MetaClientException {
        // 分区配置发生了变化,可用分区不变,选择出来的分区是0-0，不可写
        Map<String, List<Partition>> map = new HashMap<String, List<Partition>>();
        map.put(testTopic, Arrays.asList(new Partition("1-0"), new Partition("1-1"), new Partition("2-0")));
        this.selector.setConfigPartitions(map);

        Message message = this.createDefaultMessage();
        Partition partition =
                this.selector.getPartition(message.getTopic(),
                    Arrays.asList(new Partition("0-0"), new Partition("1-0"), new Partition("2-0")), message);
        Assert.assertEquals(new Partition("0-0"), partition);
    }


    @Test
    public void testGetPartition_availablePartitionsChanged_butSelectedPartitionAvailable2() throws MetaClientException {
        // 可用分区数少了(1-0),按照配置的分区选择,选出来的分区(0-0)包含在可用分区里,返回这个分区（这个分区继续可写）

        Message message = this.createDefaultMessage();
        Partition partition =
                this.selector.getPartition(message.getTopic(),
                    Arrays.asList(new Partition("0-0"), new Partition("0-1")), message);
        Assert.assertEquals(new Partition("0-0"), partition);
    }


    @Test(expected = AvailablePartitionNumException.class)
    public void testGetPartition_availablePartitionsChanged_butSelectedPartitionAvailable_expception()
            throws MetaClientException {
        // 可用分区数少了（0-0）,按照配置的分区选择,选出来的分区（0-0）不包含在可用分区里

        Message message = this.createDefaultMessage();
        this.selector.getPartition(message.getTopic(), Arrays.asList(new Partition("0-1"), new Partition("1-0")),
            message);
    }


    private Message createDefaultMessage() {
        final String topic = testTopic;
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        return message;
    }
}