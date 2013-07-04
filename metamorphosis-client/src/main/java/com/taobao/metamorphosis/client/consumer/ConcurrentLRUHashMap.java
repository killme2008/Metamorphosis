package com.taobao.metamorphosis.client.consumer;

import java.util.concurrent.locks.ReentrantLock;

import com.taobao.common.store.util.LRUHashMap;


/**
 * Concurrent LRU map
 * 
 * @author dennis
 * 
 * @param <K>
 * @param <V>
 */
public class ConcurrentLRUHashMap<K, V> {
    private final LRUHashMap<K, V> innerMap;
    private ReentrantLock lock;


    public ConcurrentLRUHashMap() {
        this(1024);
    }


    public ConcurrentLRUHashMap(int capacity) {
        this.innerMap = new LRUHashMap<K, V>(capacity, true);
        this.lock = new ReentrantLock();
    }


    public void put(K k, V v) {
        this.lock.lock();
        try {
            this.innerMap.put(k, v);
        }
        finally {
            this.lock.unlock();
        }
    }


    public V get(K k) {
        this.lock.lock();
        try {
            return this.innerMap.get(k);
        }
        finally {
            this.lock.unlock();
        }
    }

}
