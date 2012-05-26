package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 事务性消费
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-9-2
 * 
 */
public class TransactionConsumerTest extends BaseMetaTest {
    final String topic = "meta-test";
    final int maxSize = 1024 * 1024;


    @Test
    public void testTxConsumeRollback() throws Exception {
        try {
            this.producer = this.sessionFactory.createProducer(new PartitionSelector() {

                public Partition getPartition(final String topic, final List<Partition> partitions,
                        final Message message) throws MetaClientException {
                    return partitions.get(0);
                }
            });
            this.createConsumer("tx-consumer-test");
            final AtomicInteger counter = new AtomicInteger(0);
            final int sentCount = 5;
            final List<Message> msgList1 = new ArrayList<Message>();
            final List<Message> msgList2 = new ArrayList<Message>();
            this.consumer.subscribe(this.topic, this.maxSize, new MessageListener() {

                public void recieveMessages(final Message message) {
                    System.out.println("收到消息:" + new String(message.getData()));
                    message.getPartition().setAutoAck(false);
                    // 5条回滚
                    final int count = counter.incrementAndGet();
                    if (count <= sentCount) {
                        msgList1.add(message);
                    }
                    else {
                        msgList2.add(message);
                    }
                    if (count == sentCount) {
                        System.out.println("收到" + count + "条消息并回滚");
                        message.getPartition().rollback();
                    }
                    else if (count == 10) {
                        // 10条,ack
                        System.out.println("收到" + count + "条消息并确认");
                        message.getPartition().ack();
                    }
                }


                public Executor getExecutor() {
                    // TODO Auto-generated method stub
                    return null;
                }
            }).completeSubscribe();

            this.producer.publish(this.topic);
            for (int i = 0; i < sentCount; i++) {
                if (!this.producer.sendMessage(new Message(this.topic, String.valueOf(i).getBytes())).isSuccess()) {
                    throw new RuntimeException("Send message failed");
                }
            }

            while (counter.get() < 2 * sentCount) {
                Thread.sleep(1000);
            }
            Thread.sleep(2000);
            assertEquals(sentCount * 2, counter.get());
            assertEquals(msgList1, msgList2);
        }
        finally {
            this.producer.shutdown();
            this.consumer.shutdown();
        }
    }

}
