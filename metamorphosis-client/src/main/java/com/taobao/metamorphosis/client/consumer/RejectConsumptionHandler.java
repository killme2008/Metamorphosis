package com.taobao.metamorphosis.client.consumer;

import com.taobao.metamorphosis.Message;

/**
 * Created with IntelliJ IDEA.
 * User: dennis (xzhuang@avos.com)
 * Date: 13-2-5
 * Time: ионГ11:18
 */
public interface RejectConsumptionHandler {
    /**
     * Method that may be invoked by a MessageConsumer when receiveMessages cannot process a message when retry too many times.
     *
     * @param message
     * @param messageConsumer
     */
    public void rejectConsumption(Message message, MessageConsumer messageConsumer);
}
