package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Executor;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * meta集成测试_OneProducerOneConsumer
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */
public class OneProducerOneConsumerWithFilterTest extends BaseMetaTest {

    private final String topic = "filter-test";


    @Test
    public void sendConsume() throws Exception {
        this.createProducer();
        this.producer.publish(this.topic);
        // 订阅者必须指定分组
        this.createConsumer("group1");

        try {
            // 发送消息
            final int count = 1000;
            this.sendMessage(count, "hello", this.topic);

            // 订阅接收消息
            try {
                this.consumer.subscribe(this.topic, 1024 * 1024, new MessageListener() {

                    public void recieveMessages(final Message messages) {
                        OneProducerOneConsumerWithFilterTest.this.queue.add(messages);
                    }


                    public Executor getExecutor() {
                        return null;
                    }
                }).completeSubscribe();
            }
            catch (final MetaClientException e) {
                throw e;
            }
            while (this.queue.size() < count / 2) {
                Thread.sleep(1000);
                System.out.println("等待接收消息" + count / 2 + "条，目前接收到" + this.queue.size() + "条");
            }

            // 检查消息是否接收到并校验内容
            assertEquals(count / 2, this.queue.size());
            int i = 0;
            if (count != 0) {
                for (final Message msg : this.messages) {
                    if (++i % 2 == 0) {
                        assertTrue(this.queue.contains(msg));
                    }
                }
            }
            this.log.info("received message count:" + this.queue.size());
        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }

    }
}
