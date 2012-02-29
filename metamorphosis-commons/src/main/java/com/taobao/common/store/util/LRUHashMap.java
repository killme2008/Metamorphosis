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
 *   boyan <killme2008@gmail.com>
 */
package com.taobao.common.store.util;

import java.util.LinkedHashMap;


/**
 * *
 * 
 * @author dennis
 * 
 * @param <K>
 * @param <V>
 */
public class LRUHashMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxCapacity;

    static final long serialVersionUID = 438971390573954L;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private transient EldestEntryHandler<K, V> handler;

    private boolean enableLRU;


    public void setHandler(final EldestEntryHandler<K, V> handler) {
        this.handler = handler;
    }

    public interface EldestEntryHandler<K, V> {
        public boolean process(java.util.Map.Entry<K, V> eldest);
    }


    public LRUHashMap() {
        this(1000, true);
    }


    public LRUHashMap(final int maxCapacity, final boolean enableLRU) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
        this.maxCapacity = maxCapacity;
        this.enableLRU = enableLRU;
    }


    @Override
    protected boolean removeEldestEntry(final java.util.Map.Entry<K, V> eldest) {
        if (!this.enableLRU) {
            return false;
        }
        final boolean result = this.size() > maxCapacity;
        if (result && handler != null) {
            // 成功存入磁盘，即从内存移除，否则继续保留在保存
            return handler.process(eldest);
        }
        return result;
    }
}