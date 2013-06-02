package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;


/**
 * meta���ɲ���_OneProducerOneConsumer
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */
public class MesageSetRollbackOnlyTest extends BaseMetaTest {

    private final String topic = "meta-test";


    @Test(timeout = 60000)
    public void sendConsume() throws Exception {
        this.createProducer();
        this.producer.publish(this.topic);
        // �����߱���ָ������
        this.createConsumer("group1");

        try {
            // ������Ϣ
            int count = 5;
            for (int i = 0; i < count; i++) {
                this.sendMessage(1, "hello" + i, this.topic);
            }

            final ConcurrentHashMap<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();
            final AtomicInteger total = new AtomicInteger();
            // ���Ľ�����Ϣ����֤������ȷ
            this.consumer.subscribe(this.topic, 1024, new MessageListener() {

                public void recieveMessages(Message message) throws InterruptedException {
                    String body = new String(message.getData());
                    AtomicInteger counter = counterMap.get(body);
                    if (counter == null) {
                        counter = new AtomicInteger(0);
                        AtomicInteger oldCounter = counterMap.putIfAbsent(body, counter);
                        if (oldCounter != null) {
                            counter = oldCounter;
                        }
                    }
                    if (counter.incrementAndGet() <= 2) {
                        message.setRollbackOnly();
                    }
                    total.incrementAndGet();

                }


                public Executor getExecutor() {
                    // TODO Auto-generated method stub
                    return null;
                }
            }).completeSubscribe();

            while (total.get() < 3 * count) {
                Thread.sleep(1000);
                System.out.println("���յ�" + total.get() + "��Ϣ");
            }
            Thread.sleep(5000);
            assertEquals(count, counterMap.size());
            assertEquals(15, total.get());
            for (Map.Entry<String, AtomicInteger> entry : counterMap.entrySet()) {
                assertEquals(3, entry.getValue().get());
            }
        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }

    }
}
