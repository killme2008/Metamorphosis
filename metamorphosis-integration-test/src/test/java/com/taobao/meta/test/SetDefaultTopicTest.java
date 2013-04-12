package com.taobao.meta.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;


/**
 * meta集成测试_OneProducerOneConsumer
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */
public class SetDefaultTopicTest extends BaseMetaTest {

    private final String topic = "meta-test";


    @Override
    @Before
    public void setUp() throws Exception {
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        this.sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        this.startServer("server3");
        System.out.println("before run");
    }


    @Override
    @After
    public void tearDown() throws Exception {
        this.sessionFactory.shutdown();
        Utils.stopServers(this.brokers);
        System.out.println("after run");
    }


    @Test
    public void sendConsume() throws Exception {
        this.createProducer();
        this.producer.setDefaultTopic(this.topic);
        // 订阅者必须指定分组
        this.createConsumer("group1");

        try {
            // 发送消息
            final int count = 5;
            // 发送到不存在的topic
            this.sendMessage(count, "hello", "SetDefaultTopicTest");

            // 订阅接收消息并验证数据正确
            this.subscribe("SetDefaultTopicTest", 1024 * 1024, count);
        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }

    }
}
