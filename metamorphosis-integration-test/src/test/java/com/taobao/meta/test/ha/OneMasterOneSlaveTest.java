package com.taobao.meta.test.ha;

import org.junit.After;
import org.junit.Test;

import com.taobao.metamorphosis.EnhancedBroker;


/**
 * 
 * @author 无花
 * @since 2011-7-12 下午02:31:37
 */

public class OneMasterOneSlaveTest extends HABaseMetaTest {
    private final String topic = "meta-test";
    private EnhancedBroker slaveBroker;


    @Test
    public void sendConsume() throws Exception {
        // 先启动一台master，观察是否正确发送和接收消息；然后挂上一台slave，查看消费者负载均衡变化和是否正确接收消息；
        // 然后关闭slave,再观察;再启动slave观察

        // start master
        super.startServer("server1");
        super.createProducer();
        this.producer.publish(this.topic);
        super.createConsumer("group1");

        final int count = 5;
        super.sendMessage(count, "hello", this.topic);
        super.subscribe(this.topic, 1024 * 1024, count);

        // start slave
        this.slaveBroker = super.startSlaveServers("slave1-1", false, true);
        this.log.info("------------slave started--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 2);

        // stop slave
        this.slaveBroker.stop();
        this.log.info("------------slave stop--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 3);

        // start slave again
        this.slaveBroker = super.startSlaveServers("slave1-1", false, false);
        this.log.info("------------slave started--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 4);
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
