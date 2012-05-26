package com.taobao.meta.test.ha;

import org.junit.Test;


/**
 * 
 * @author 无花
 * @since 2011-7-12 下午01:48:25
 */

public class OneMasterNProducerNConsumerTest extends HABaseMetaTest {

    private final String topic = "meta-test";


    @Test
    public void sendConsume() throws Exception {
        this.startServer("server2");
        this.create_nProducer(10);

        // 发送消息
        final int count = 5;
        this.sendMessage_nProducer(count, "hello", this.topic, 10);
        // 订阅接收消息并验证数据正确
        this.subscribe_nConsumer(this.topic, 1024 * 1024, count, 10, 10);
    }
}
