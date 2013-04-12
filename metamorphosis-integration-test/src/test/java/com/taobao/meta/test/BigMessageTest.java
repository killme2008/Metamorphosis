package com.taobao.meta.test;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;


/**
 * 收发巨型消息的测试
 * 
 * @author 无花
 * @since 2011-8-17 下午5:41:41
 */
@Ignore
public class BigMessageTest extends BaseMetaTest {
    private final String topic = "meta-test";


    @Override
    @Before
    public void setUp() throws Exception {
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        this.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        this.startServer("bigmessageserver");
        System.out.println("before run");
    }


    @Test
    public void sendConsume() throws Exception {

        this.createProducer();
        this.producer.publish(this.topic);
        // 订阅者必须指定分组
        this.createConsumer("group1");

        try {
            // 发送每条2M的消息
            final int count = 50;
            this.sendMessage(count, Utils.getData(2 * 1024 * 1024), this.topic);

            // 订阅接收消息并验证数据正确
            this.subscribe(this.topic, 5 * 1024 * 1024, count);

        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }

    }
}
