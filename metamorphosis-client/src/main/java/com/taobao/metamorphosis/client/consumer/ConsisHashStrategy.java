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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;


/**
 * 基于一致性哈希的负载均衡策略：</br>
 * <ul>
 * <li>将所有consumer组织成一个环</li>
 * <li>将所有分区根据hash值插入到环上</li>
 * <li>获取指定consumer前面，前一个consumer之后的分区列表作为结果</li>
 * </ul>
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public class ConsisHashStrategy implements LoadBalanceStrategy {
    // 虚拟节点数目
    static final int NUM_REPS = 160;
    HashAlgorithm alg = HashAlgorithm.KETAMA_HASH;


    /**
     * Get the md5 of the given key.
     */
    public static byte[] computeMd5(final String k) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        }
        catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        md5.update(k.getBytes());
        return md5.digest();
    }


    @Override
    public List<String> getPartitions(final String topic, final String consumerId, final List<String> curConsumers,
            final List<String> curPartitions) {

        final TreeMap<Long, String> consumerMap = this.buildConsumerMap(curConsumers);

        final Set<String> rt = new HashSet<String>();
        // 根据partition查找对应的consumer
        for (final String partition : curPartitions) {
            final String targetConsumer = this.findConsumerByPartition(consumerMap, partition);
            // 保存本consumer需要挂载的分区
            if (consumerId.equals(targetConsumer)) {
                rt.add(partition);
            }
        }
        return new ArrayList<String>(rt);
    }


    private String findConsumerByPartition(final TreeMap<Long, String> consumerMap, final String partition) {
        final Long hash = this.alg.hash(partition);
        Long target = hash;
        if (!consumerMap.containsKey(hash)) {
            target = consumerMap.ceilingKey(hash);
            // if (hash == null) {
            // target = consumerMap.floorKey(hash);
            // }
            // else {
            // final Long floor = consumerMap.floorKey(hash);
            // if (floor != null) {
            // target = Math.abs(hash - floor) > Math.abs(hash - target) ?
            // target : floor;
            // }
            // }
            if (target == null && !consumerMap.isEmpty()) {
                target = consumerMap.firstKey();
            }
        }
        final String targetConsumer = consumerMap.get(target);
        return targetConsumer;
    }


    private TreeMap<Long, String> buildConsumerMap(final List<String> curConsumers) {
        final TreeMap<Long/* hash */, String/* consumerId */> consumerMap = new TreeMap<Long, String>();
        for (final String consumer : curConsumers) {
            if (this.alg == HashAlgorithm.KETAMA_HASH) {
                for (int i = 0; i < NUM_REPS / 4; i++) {
                    final byte[] digest = HashAlgorithm.computeMd5(consumer + "-" + i);
                    for (int h = 0; h < 4; h++) {
                        final long k =
                                (long) (digest[3 + h * 4] & 0xFF) << 24 | (long) (digest[2 + h * 4] & 0xFF) << 16
                                        | (long) (digest[1 + h * 4] & 0xFF) << 8 | digest[h * 4] & 0xFF;
                        consumerMap.put(k, consumer);
                    }

                }
            }
            else {
                for (int i = 0; i < NUM_REPS; i++) {
                    final long key = this.alg.hash(consumer + "-" + i);
                    consumerMap.put(key, consumer);
                }
            }

        }
        return consumerMap;
    }
}