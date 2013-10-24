package com.taobao.metamorphosis.example.filter;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;


public class AttributeMessageFilter implements ConsumerMessageFilter {

    @Override
    public boolean accept(String group, Message message) {
        if (message.getAttribute() == null) {
            return false;
        }
        return message.getAttribute().equals("accept");
    }

}
