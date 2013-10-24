package com.taobao.meta.test.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.meta.test.BaseMetaTest;
import com.taobao.metamorphosis.client.extension.spring.MessageBuilder;
import com.taobao.metamorphosis.client.extension.spring.MetaqTemplate;
import com.taobao.metamorphosis.client.producer.SendResult;


public class SrpingAPITest extends BaseMetaTest {

    @Test(timeout = 60000)
    public void sendConsume() throws Exception {
        this.createProducer();

        ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
        // use template to send messages.
        final String topic = "meta-test";
        MetaqTemplate template = (MetaqTemplate) context.getBean("metaqTemplate");
        int count = 100;
        for (int i = 0; i < count; i++) {
            SendResult result =
                    template.send(MessageBuilder.withTopic(topic).withBody(new Trade(i, "test", i, "test")));
            assertTrue(result.isSuccess());
        }
        TradeMessageListener listener = (TradeMessageListener) context.getBean("messageListener");
        while (listener.counter.get() != count) {
            Thread.sleep(100);
        }
        assertEquals(listener.counter.get(), count);
    }
}
