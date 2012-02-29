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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.common.store.journal.IndexMap;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.LRUHashMap;


/**
 * 
 * ª˘”⁄LRUµƒIndexMap£¨ø…Ω´LRUÃÊªª≥ˆ¿¥µƒOpItem¥Ê¥¢”⁄¥≈≈Ãª∫¥Ê
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-20 …œŒÁ11:04:37
 */

public class LRUIndexMap implements IndexMap {
    private final Lock lock = new ReentrantLock();
    private final LRUHashMap<BytesKey, OpItem> map;
    private final NotifyEldestEntryHandler handler;
    private final boolean enableLRU;


    public LRUIndexMap(final int capacity, final String cacheFilePath, final boolean enableLRU) throws IOException {
        this.enableLRU = enableLRU;
        map = new LRUHashMap<BytesKey, OpItem>(capacity, enableLRU);
        handler = new NotifyEldestEntryHandler(capacity, cacheFilePath);
        map.setHandler(handler);
    }


    @Override
    public void close() throws IOException {
        this.lock.lock();
        try {
            this.handler.close();
        }
        finally {
            this.lock.unlock();
        }
    }


    public LRUHashMap<BytesKey, OpItem> getMap() {
        return map;
    }


    public NotifyEldestEntryHandler getHandler() {
        return handler;
    }


    @Override
    public boolean containsKey(final BytesKey key) {
        this.lock.lock();
        try {

            return map.containsKey(key) || (enableLRU && this.handler.getDiskMap().get(key) != null);
        }
        catch (final IOException e) {
            throw new IllegalStateException("≤È—ØKey ß∞‹", e);
        }
        finally {
            this.lock.unlock();
        }
    }


    @Override
    public OpItem get(final BytesKey key) {
        this.lock.lock();
        try {
            OpItem result = map.get(key);
            if (result == null && enableLRU) {
                result = handler.getDiskMap().get(key);
            }
            return result;
        }
        catch (final IOException e) {
            throw new IllegalStateException("∑√Œ ¥≈≈Ãª∫¥Ê ß∞‹", e);
        }
        finally {
            this.lock.unlock();
        }

    }

    class LRUIndexMapItreator implements Iterator<BytesKey> {

        private final Iterator<BytesKey> mapIt;
        private final Iterator<BytesKey> diskMapIt;
        private volatile boolean enterDisk;
        private BytesKey currentKey;


        public LRUIndexMapItreator(final Iterator<BytesKey> mapIt, final Iterator<BytesKey> diskMapIt) {
            super();
            this.mapIt = mapIt;
            this.diskMapIt = diskMapIt;
        }


        @Override
        public boolean hasNext() {
            lock.lock();
            try {
                if (mapIt.hasNext()) {
                    return true;
                }
                if (enableLRU) {
                    if (!enterDisk) {
                        enterDisk = true;
                    }
                    return diskMapIt.hasNext();
                }
                return false;
            }
            finally {
                lock.unlock();
            }
        }


        @Override
        public BytesKey next() {
            lock.lock();
            try {
                BytesKey result = null;
                if (!enterDisk) {
                    result = mapIt.next();
                }
                else {
                    result = diskMapIt.next();
                }
                this.currentKey = result;
                return result;
            }
            finally {
                lock.unlock();
            }
        }


        @Override
        public void remove() {
            lock.lock();
            try {
                if (currentKey == null) {
                    throw new IllegalStateException("The next method is not been called");
                }
                LRUIndexMap.this.remove(this.currentKey);
            }
            finally {
                lock.unlock();
            }
        }

    }


    @Override
    public Iterator<BytesKey> keyIterator() {
        lock.lock();
        try {
            return new LRUIndexMapItreator(new HashSet<BytesKey>(map.keySet()).iterator(), handler.getDiskMap()
                .iterator());
        }
        finally {
            lock.unlock();
        }
    }


    @Override
    public void put(final BytesKey key, final OpItem opItem) {
        lock.lock();
        try {
            this.map.put(key, opItem);
        }
        finally {
            lock.unlock();
        }
    }


    @Override
    public void putAll(final Map<BytesKey, OpItem> map) {
        lock.lock();
        try {
            this.map.putAll(map);
        }
        finally {
            lock.unlock();
        }
    }


    @Override
    public void remove(final BytesKey key) {
        lock.lock();
        try {
            final OpItem result = map.remove(key);
            if (result == null && enableLRU) {
                try {
                    handler.getDiskMap().remove(key);
                }
                catch (final IOException e) {
                    throw new IllegalStateException("∑√Œ ¥≈≈Ãª∫¥Ê ß∞‹", e);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }


    @Override
    public int size() {
        lock.lock();
        try {
            return map.size() + handler.getDiskMap().size();
        }
        finally {
            lock.unlock();
        }
    }

}