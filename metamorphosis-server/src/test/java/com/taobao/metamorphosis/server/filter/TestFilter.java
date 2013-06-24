package com.taobao.metamorphosis.server.filter;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;


public class TestFilter implements ConsumerMessageFilter {

    @Override
    public boolean accept(String group, Message message) {
        return true;
    }

}
