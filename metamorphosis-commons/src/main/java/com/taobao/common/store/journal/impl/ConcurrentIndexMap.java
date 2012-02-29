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
package com.taobao.common.store.journal.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.common.store.journal.IndexMap;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;


/**
 * 
 * @author boyan *
 */

public class ConcurrentIndexMap implements IndexMap {
    private final ConcurrentHashMap<BytesKey, OpItem> map;


    public ConcurrentIndexMap() {
        this.map = new ConcurrentHashMap<BytesKey, OpItem>();
    }


    @Override
    public boolean containsKey(final BytesKey key) {
        return this.map.containsKey(key);
    }


    @Override
    public OpItem get(final BytesKey key) {
        return this.map.get(key);
    }


    @Override
    public Iterator<BytesKey> keyIterator() {
        return this.map.keySet().iterator();
    }


    @Override
    public void put(final BytesKey key, final OpItem opItem) {
        this.map.put(key, opItem);
    }


    @Override
    public void putAll(final Map<BytesKey, OpItem> map) {
        this.map.putAll(map);
    }


    @Override
    public void remove(final BytesKey key) {
        this.map.remove(key);
    }


    @Override
    public int size() {
        return this.map.size();
    }


    @Override
    public void close() throws IOException {
        this.map.clear();
    }

}