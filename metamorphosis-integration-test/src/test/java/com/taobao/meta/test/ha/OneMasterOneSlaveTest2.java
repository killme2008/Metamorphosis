package com.taobao.meta.test.ha;

import org.junit.After;
import org.junit.Test;

import com.taobao.meta.test.Utils;
import com.taobao.metamorphosis.EnhancedBroker;


/**
 * 
 * @author 无花
 * @since 2011-7-12 下午04:29:02
 */

public class OneMasterOneSlaveTest2 extends HABaseMetaTest {
    private final String topic = "meta-test";
    private EnhancedBroker slaveBroker;


    @Test
    public void sendConsume() throws Exception {
        // 先启动一台master，观察是否正确发送和接收消息；然后挂上一台slave，查看消费者负载均衡变化和是否正确接收消息；
        // 接着发送消息之后等待数据同步到slave再关闭master,再观察消息接收情况;再启动master

        // start master
        super.startServer("server1");
        super.createProducer();
        this.producer.publish(this.topic);
        super.createConsumer("group1");

        final int count = 5;
        super.sendMessage(count, "hello", this.topic);
        super.subscribe(this.topic, 1024 * 1024, count);

        // start slave
        this.log.info("------------start slave...--------------");
        this.slaveBroker = super.startSlaveServers("slave1-1", false, true);
        super.sendMessage(count, "hello", this.topic);
        // stop master
        Thread.sleep(6000);
        this.log.info("------------stop master...--------------");
        Utils.stopServers(super.brokers);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 2);

        // start master again
        this.log.info("------------start master again...--------------");
        super.startServer("server1", false, false);
        Thread.sleep(2000);
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 3);
        this.log.info("------------end--------------");
        Thread.sleep(3000);
    }


    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (this.slaveBroker != null) {
            this.slaveBroker.stop();
        }

    }

}
