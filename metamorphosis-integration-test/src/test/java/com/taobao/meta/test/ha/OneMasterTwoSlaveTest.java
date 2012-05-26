package com.taobao.meta.test.ha;

import java.util.Arrays;

import org.junit.After;
import org.junit.Test;

import com.taobao.meta.test.Utils;
import com.taobao.metamorphosis.EnhancedBroker;


/**
 * 
 * @author 无花
 * @since 2011-7-12 下午04:28:29
 */
// note:一台server突然关闭后出现连接异常是正常的,实际运行中负载均衡之后错误就会消失了
public class OneMasterTwoSlaveTest extends HABaseMetaTest {

    private final String topic = "meta-test";
    private EnhancedBroker slaveBroker1;
    private EnhancedBroker slaveBroker2;


    @Test
    public void sendConsume() throws Exception {
        // 先启动一台master，观察是否正确发送和接收消息；然后挂上一台slave，查看消费者负载均衡变化和是否正确接收消息；
        // 再挂上一台slave
        // 然后关闭slave,再观察;再启动slave观察

        // start master
        super.startServer("server1");
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

        // stop slave1
        this.slaveBroker1.stop();
        this.log.info("------------slave1 stop--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 4);

        // start slave1 again
        this.slaveBroker1 = super.startSlaveServers("slave1-1", false, false);
        this.log.info("------------slave1 started again--------------");
        super.sendMessage(count, "hello", this.topic);
        super.subscribeRepeatable(this.topic, 1024 * 1024, count * 5);

        Thread.sleep(3000);

    }


    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        Utils.stopWrapperServers(Arrays.asList(this.slaveBroker1, this.slaveBroker2));
    }
}
