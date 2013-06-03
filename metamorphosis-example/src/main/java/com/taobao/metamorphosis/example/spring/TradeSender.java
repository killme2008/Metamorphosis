package com.taobao.metamorphosis.example.spring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.metamorphosis.client.extension.spring.MessageBuilder;
import com.taobao.metamorphosis.client.extension.spring.MetaqTemplate;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.example.spring.messages.Trade;


public class TradeSender {
    public static void main(final String[] args) throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext("bean.xml");
        // use template to send messages.
        final String topic = "test";
        MetaqTemplate template = (MetaqTemplate) context.getBean("metaqTemplate");

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        long tradeId = 0;
        int money = 1000;
        while ((line = readLine(reader)) != null) {
            // send message
            final SendResult sendResult =
                    template.send(MessageBuilder.withTopic(topic).withBody(new Trade(tradeId++, line, money++, line)));
            // check result
            if (!sendResult.isSuccess()) {
                System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
            }
            else {
                System.out.println("Send message successfully,sent to " + sendResult.getPartition());
            }
        }
    }


    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }
}
