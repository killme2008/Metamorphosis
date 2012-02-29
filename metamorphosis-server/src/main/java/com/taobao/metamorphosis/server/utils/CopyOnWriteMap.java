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
 */

/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License. 
 *  
 */
package com.taobao.metamorphosis.server.utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * 来自于mina项目<a href="http://mina.apache.org">Apache MINA Project</a>
 * 
 * @modification by 无花
 * @since 2011-8-11 下午3:08:09
 */

public class CopyOnWriteMap<K, V> implements Map<K, V>, Cloneable, Serializable {
    private static final long serialVersionUID = 788933834504546710L;

    private volatile Map<K, V> internalMap;


    public CopyOnWriteMap() {
        this.internalMap = new HashMap<K, V>();
    }


    public CopyOnWriteMap(final int initialCapacity) {
        this.internalMap = new HashMap<K, V>(initialCapacity);
    }


    public CopyOnWriteMap(final Map<K, V> data) {
        this.internalMap = new HashMap<K, V>(data);
    }


    @Override
    public V put(final K key, final V value) {
        synchronized (this) {
            final Map<K, V> newMap = new HashMap<K, V>(this.internalMap);
            final V val = newMap.put(key, value);
            this.internalMap = newMap;
            return val;
        }
    }


    @Override
    public V remove(final Object key) {
        synchronized (this) {
            final Map<K, V> newMap = new HashMap<K, V>(this.internalMap);
            final V val = newMap.remove(key);
            this.internalMap = newMap;
            return val;
        }
    }


    @Override
    public void putAll(final Map<? extends K, ? extends V> newData) {
        synchronized (this) {
            final Map<K, V> newMap = new HashMap<K, V>(this.internalMap);
            newMap.putAll(newData);
            this.internalMap = newMap;
        }
    }


    @Override
    public void clear() {
        synchronized (this) {
            this.internalMap = new HashMap<K, V>();
        }
    }


    @Override
    public int size() {
        return this.internalMap.size();
    }


    @Override
    public boolean isEmpty() {
        return this.internalMap.isEmpty();
    }


    @Override
    public boolean containsKey(final Object key) {
        return this.internalMap.containsKey(key);
    }


    @Override
    public boolean containsValue(final Object value) {
        return this.internalMap.containsValue(value);
    }


    @Override
    public V get(final Object key) {
        return this.internalMap.get(key);
    }


    @Override
    public Set<K> keySet() {
        return this.internalMap.keySet();
    }


    @Override
    public Collection<V> values() {
        return this.internalMap.values();
    }


    @Override
    public Set<Entry<K, V>> entrySet() {
        return this.internalMap.entrySet();
    }


    @Override
    public Object clone() {
        try {
            return super.clone();
        }
        catch (final CloneNotSupportedException e) {
            throw new InternalError();
        }
    }
}