package com.taobao.metamorphosis.example.cache;

import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.client.consumer.MessageIdCache;


public class MemcachedMessageIdCache implements MessageIdCache {
    private final MemcachedClient memcachedClient;
    private int expireInSeconds = 60;
    private static final Log log = LogFactory.getLog(MemcachedMessageIdCache.class);


    public MemcachedMessageIdCache(MemcachedClient client) {
        this.memcachedClient = client;
    }


    public void setExpireInSeconds(int expireInSeconds) {
        this.expireInSeconds = expireInSeconds;
    }


    public int getExpireInSeconds() {
        return this.expireInSeconds;
    }


    @Override
    public void put(String key, Byte exists) {
        try {
            this.memcachedClient.set(key, this.expireInSeconds, exists);
        }
        catch (MemcachedException e) {
            log.error("Added message id cache failed", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (TimeoutException e) {
            log.error("Added message id cache timeout", e);
        }

    }


    @Override
    public Byte get(String key) {
        try {
            return this.memcachedClient.get(key);
        }
        catch (MemcachedException e) {
            log.error("Get item from message id cache failed", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (TimeoutException e) {
            log.error("Get item from message id cache timeout", e);
        }
        return null;
    }

}
