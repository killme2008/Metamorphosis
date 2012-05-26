package com.taobao.meta.test.ha;

import org.junit.Test;


/**
 * 
 * @author 无花
 * @since 2011-7-12 上午11:08:25
 */

public class OneMasterOneProducerOneConsumerTest extends HABaseMetaTest {

    private final String topic = "meta-test";


    @Test
    public void sendConsume() throws Exception {
        super.startServer("server1");
        super.createProducer();
        this.producer.publish(this.topic);
        // 订阅者必须指定分组
        super.createConsumer("group1");

        // 发送消息
        final int count = 5;
        super.sendMessage(count, "hello", this.topic);

        // 订阅接收消息并验证数据正确
        super.subscribe(this.topic, 1024 * 1024, count);

    }

}
