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
public class ConcurrentLRUHashMap implements MessageIdCache {
    private final LRUHashMap<String, Byte> innerMap;
    private final ReentrantLock lock;


    public ConcurrentLRUHashMap() {
        this(1024);
    }


    public int size() {
        this.lock.lock();
        try {
            return this.innerMap.size();
        }
        finally {
            this.lock.unlock();
        }
    }


    public ConcurrentLRUHashMap(int capacity) {
        this.innerMap = new LRUHashMap<String, Byte>(capacity, true);
        this.lock = new ReentrantLock();
    }


    @Override
    public void put(String k, Byte v) {
        this.lock.lock();
        try {
            this.innerMap.put(k, v);
        }
        finally {
            this.lock.unlock();
        }
    }


    @Override
    public Byte get(String k) {
        this.lock.lock();
        try {
            return this.innerMap.get(k);
        }
        finally {
            this.lock.unlock();
        }
    }

}
