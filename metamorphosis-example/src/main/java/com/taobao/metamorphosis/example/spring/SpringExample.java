package com.taobao.metamorphosis.example.spring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Executor;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;


public class SpringExample {
    public static void main(final String[] args) throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext("bean.xml");
        final String topic = "meta-test";
        final MessageConsumer consumer = (MessageConsumer) context.getBean("consumer");
        // get consumer and subscribe topics
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
            }

            @Override
            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }
        }).completeSubscribe();

        // get producer and publish topics
        final MessageProducer producer = (MessageProducer) context.getBean("producer");
        // publish topic
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while ((line = readLine(reader)) != null) {
            // send message
            final SendResult sendResult = producer.sendMessage(new Message(topic, line.getBytes()));
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
