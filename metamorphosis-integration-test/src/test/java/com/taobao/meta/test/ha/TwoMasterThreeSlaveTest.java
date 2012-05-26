package com.taobao.meta.test.ha;

import java.util.Arrays;

import org.junit.After;
import org.junit.Test;

import com.taobao.meta.test.Utils;
import com.taobao.metamorphosis.EnhancedBroker;


/**
 * 
 * @author 无花
 * @since 2011-7-12 下午07:50:30
 */
// note:一台server突然关闭后出现连接异常是正常的,实际运行中负载均衡之后错误就会消失了
public class TwoMasterThreeSlaveTest extends HABaseMetaTest {
    private final String topic = "meta-test";
    private EnhancedBroker slaveBroker1;
    private EnhancedBroker slaveBroker2;
    private EnhancedBroker slaveBroker3;


    @Test
    public void sendConsume() throws Exception {
        super.startServer("server1");
        super.startServer("server2");
        super.createProducer();
        this.producer.publish(this.topic);
        super.createConsumer("group1");

        final int count = 5;
        super.sendMessage(count, "hello", this.topic);
        super.subscribe(this.topic, 1024 * 1024, count);

        // start slave1
        this.slaveBroker1 = super.startSlaveServers("slave1-1", false, true);
        this.log.info("------------slave1 started--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 2);

        // start slave2
        this.slaveBroker2 = super.startSlaveServers("slave1-2", false, true);
        this.log.info("------------slave2 started--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 3);

        // start slave3
        this.slaveBroker3 = super.startSlaveServers("slave2-1", false, true);
        this.log.info("------------slave3 started--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 4);

        // stop slave1
        this.slaveBroker1.stop();
        this.log.info("------------slave1 stop--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 5);

        // start slave1 again
        this.slaveBroker1 = super.startSlaveServers("slave1-1", false, false);
        this.log.info("------------slave1 started again--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 6);

        Thread.sleep(3000);

    }


    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        Utils.stopWrapperServers(Arrays.asList(this.slaveBroker1, this.slaveBroker2, this.slaveBroker3));
        Thread.sleep(3000);
    }

}
