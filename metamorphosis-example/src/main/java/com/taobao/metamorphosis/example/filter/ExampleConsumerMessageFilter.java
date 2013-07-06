package com.taobao.metamorphosis.example.filter;

import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;


public class ExampleConsumerMessageFilter implements ConsumerMessageFilter {

    AtomicInteger counter = new AtomicInteger(0);
    @Override
    public boolean accept(String group, Message message) {
        return this.counter.incrementAndGet() % 2 == 0;
    }

}
