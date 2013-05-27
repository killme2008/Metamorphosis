package com.taobao.meta.test.spring;

import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.metamorphosis.client.extension.spring.DefaultMessageListener;
import com.taobao.metamorphosis.client.extension.spring.MetaqMessage;


/**
 * Process trade messages listener.
 * 
 * @author dennis
 * 
 */
public class TradeMessageListener extends DefaultMessageListener<Trade> {

    AtomicInteger counter = new AtomicInteger();


    @Override
    public void onReceiveMessages(MetaqMessage<Trade> msg) {
        if (msg.getBody() == null) {
            throw new RuntimeException();
        }
        this.counter.incrementAndGet();
    }

}
