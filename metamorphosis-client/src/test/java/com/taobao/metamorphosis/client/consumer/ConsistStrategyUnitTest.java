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
package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;

import com.taobao.gecko.core.util.RemotingUtils;
import com.taobao.metamorphosis.client.BaseStrategyUnitTest;


@Ignore
public class ConsistStrategyUnitTest extends BaseStrategyUnitTest {

    private final LoadBalanceStrategy strategy = new ConsisHashStrategy();

    private final AtomicInteger counter = new AtomicInteger(0);


    @Override
    public List<String> createConsumers(final int num) throws Exception {
        final List<String> rt = new ArrayList<String>();
        for (int i = 0; i < num; i++) {
            rt.add("consumer-" + RemotingUtils.getLocalHostAddress() + "-" + System.currentTimeMillis() + "-"
                    + this.counter.incrementAndGet());
        }
        return rt;
    }


    @Test
    public void testGetPartitions_4consumers_10partitions() throws Exception {
        final String topic = "test";
        final int consumers = 50;
        final List<String> curConsumers = this.createConsumers(consumers);
        final List<String> curPartitions = this.createPartitions(topic, 100);
        int count = 0;
        for (int i = 0; i < consumers; i++) {
            final List<String> partitions =
                    this.strategy.getPartitions(topic, curConsumers.get(i), curConsumers, curPartitions);
            System.out.println(curConsumers.get(i) + " " + partitions + "  ");
            if (partitions.isEmpty()) {
                count++;
            }
        }
        System.out.println(count);

        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-0",
        // curConsumers, curPartitions), "test-0",
        // "test-1", "test-2");
        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-1",
        // curConsumers, curPartitions), "test-3",
        // "test-4", "test-5");
        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-2",
        // curConsumers, curPartitions), "test-6",
        // "test-7");
        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-3",
        // curConsumers, curPartitions), "test-8",
        // "test-9");

        assertTrue(this.strategy.getPartitions(topic, "consumer-100", curConsumers, curPartitions).isEmpty());
    }


    @Test
    public void testGetPartitions_3consumers_10partitions() throws Exception {
        System.out.println();
        final String topic = "test";
        final int consumers = 39;
        final List<String> curConsumers = this.createConsumers(consumers);
        final List<String> curPartitions = this.createPartitions(topic, 50);
        int count = 0;
        for (int i = 0; i < consumers; i++) {
            final List<String> partitions =
                    this.strategy.getPartitions(topic, curConsumers.get(i), curConsumers, curPartitions);
            System.out.print(partitions + "  ");
            if (partitions.isEmpty()) {
                count++;
            }
        }
        System.out.println(count);
        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-0",
        // curConsumers, curPartitions), "test-4",
        // "test-5", "test-6", "test-9");
        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-1",
        // curConsumers, curPartitions), "test-8",
        // "test-2", "test-0");
        // this.assertInclude(this.strategy.getPartitions(topic, "consumer-2",
        // curConsumers, curPartitions), "test-7",
        // "test-3", "test-1");
        //
        // assertTrue(this.strategy.getPartitions(topic, "consumer-100",
        // curConsumers, curPartitions).isEmpty());
    }
}