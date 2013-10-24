package com.taobao.metamorphosis.example.spring;

import com.taobao.metamorphosis.client.extension.spring.DefaultMessageListener;
import com.taobao.metamorphosis.client.extension.spring.MetaqMessage;
import com.taobao.metamorphosis.example.spring.messages.Trade;


/**
 * Process trade messages listener.
 * 
 * @author dennis
 * 
 */
public class TradeMessageListener extends DefaultMessageListener<Trade> {

    @Override
    public void onReceiveMessages(MetaqMessage<Trade> msg) {
        Trade trade = msg.getBody();
        System.out.println("receive trade message:" + trade);
    }

}
