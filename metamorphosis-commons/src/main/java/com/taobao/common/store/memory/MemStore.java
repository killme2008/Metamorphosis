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
 *   dogun (yuexuqiang at gmail.com)
 */
package com.taobao.common.store.memory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.common.store.Store;
import com.taobao.common.store.util.BytesKey;


/**
 * 
 * 
 * @author dogun (yuexuqiang at gmail.com)
 * 
 */
public class MemStore implements Store {
    private final Map<BytesKey, byte[]> datas = new ConcurrentHashMap<BytesKey, byte[]>();


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#add(byte[], byte[])
     */
    @Override
    public void add(final byte[] key, final byte[] data) throws IOException {
        this.datas.put(new BytesKey(key), data);
    }


    @Override
    public void add(final byte[] key, final byte[] data, final boolean force) throws IOException {
        this.datas.put(new BytesKey(key), data);

    }


    @Override
    public boolean remove(final byte[] key, final boolean force) throws IOException {
        return null != this.datas.remove(new BytesKey(key));
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#get(byte[])
     */
    @Override
    public byte[] get(final byte[] key) throws IOException {
        return this.datas.get(new BytesKey(key));
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#iterator()
     */
    @Override
    public Iterator<byte[]> iterator() throws IOException {
        final Iterator<BytesKey> it = this.datas.keySet().iterator();
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }


            @Override
            public byte[] next() {
                final BytesKey key = it.next();
                if (null == key) {
                    return null;
                }
                return key.getData();
            }


            @Override
            public void remove() {
                it.remove();
            }
        };
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#remove(byte[])
     */
    @Override
    public boolean remove(final byte[] key) throws IOException {
        return null != this.datas.remove(new BytesKey(key));
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#size()
     */
    @Override
    public int size() {
        return this.datas.size();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#update(byte[], byte[])
     */
    @Override
    public boolean update(final byte[] key, final byte[] data) throws IOException {
        this.datas.put(new BytesKey(key), data);
        return true;
    }


    @Override
    public void close() throws IOException {
        // nodo
    }


    @Override
    public long getMaxFileCount() {
        return Long.MAX_VALUE;
    }


    @Override
    public void setMaxFileCount(final long maxFileCount) {
    }
}