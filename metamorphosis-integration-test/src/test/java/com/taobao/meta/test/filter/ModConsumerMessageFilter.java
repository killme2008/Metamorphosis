package com.taobao.meta.test.filter;

import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;


public class ModConsumerMessageFilter implements ConsumerMessageFilter {

    private final AtomicLong counter = new AtomicLong(0);


    public boolean accept(String group, Message message) {
        boolean accept = this.counter.incrementAndGet() % 2 == 0;
        return accept;
    }

}
