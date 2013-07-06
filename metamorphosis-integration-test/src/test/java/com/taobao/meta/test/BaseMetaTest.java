package com.taobao.meta.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.taobao.gecko.core.util.ResourcesUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;


/**
 * meta集成测试基础类
 * 
 * @author gongyangyu(gongyangyu@taobao.com)
 * 
 */
@Ignore
public abstract class BaseMetaTest {
    protected final Log log = LogFactory.getLog(this.getClass());
    protected final List<MetaMorphosisBroker> brokers = new ArrayList<MetaMorphosisBroker>();
    protected MetaMessageSessionFactory sessionFactory;

    protected final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<Message>();

    protected MessageProducer producer;
    protected MessageConsumer consumer;
    protected List<Message> messages;
    protected List<MessageConsumer> consumerList;
    protected List<MessageProducer> producerList;


    public void createProducer() {
        this.producer = this.sessionFactory.createProducer();
    }


    public void create_nProducer(final int num) {
        this.producerList = new ArrayList<MessageProducer>();
        for (int i = 0; i < num; i++) {
            this.producerList.add(i, this.sessionFactory.createProducer());
        }
    }


    public void createConsumer(final String group) {
        this.consumer = this.sessionFactory.createConsumer(new ConsumerConfig(group));
    }


    public void createConsumer2() {
        this.consumer = this.sessionFactory.createConsumer(new ConsumerConfig());
    }

    MetaClientConfig metaClientConfig;

    @Before
    public void setUp() throws Exception {
        this.metaClientConfig = new MetaClientConfig();
        this.sessionFactory = new MetaMessageSessionFactory(this.metaClientConfig);
        this.startServer("server1");
        System.out.println("before run");
    }


    @After
    public void tearDown() throws Exception {
        this.sessionFactory.shutdown();
        Utils.stopServers(this.brokers);
        System.out.println("after run");
    }


    public MetaMorphosisBroker startServer(final String name) throws Exception {
        return this.startServer(name, true, true);
    }


    protected MetaMorphosisBroker startServer(final String name, final boolean isClearConsumerInfo,
            final boolean isCleanData) throws Exception {
        final MetaConfig metaConfig = this.metaConfig(name);
        metaConfig.setDashboardHttpPort(metaConfig.getServerPort() - 20);
        if (isCleanData) {
            Utils.clearDataDir(metaConfig);
        }
        final MetaMorphosisBroker broker = new MetaMorphosisBroker(metaConfig);
        if (isClearConsumerInfo) {
            Utils.clearConsumerInfoInZk(broker.getBrokerZooKeeper().getZkClient(), broker.getBrokerZooKeeper()
                .getMetaZookeeper());
        }
        broker.start();
        this.brokers.add(broker);
        return broker;
    }


    protected MetaConfig metaConfig(final String name) throws IOException, FileNotFoundException {
        final File file = ResourcesUtils.getResourceAsFile(name + ".ini");
        if (file == null || !file.exists()) {
            throw new FileNotFoundException("找不到配置文件" + name + ".ini");
        }
        final MetaConfig metaConfig = new MetaConfig();
        final File dataDir = new File(System.getProperty("user.home") + File.separator + "meta");
        if (!dataDir.exists()) {
            dataDir.mkdir();
        }

        metaConfig.loadFromFile(file.getAbsolutePath());
        metaConfig.setDataPath(metaConfig.getDataPath() + File.separator + name);
        for (final TopicConfig topicConfig : metaConfig.getTopicConfigMap().values()) {
            topicConfig.setDataPath(metaConfig.getDataPath());
        }
        return metaConfig;
    }


    public void sendMessage(final int count, final String strdata, final String topic) throws Exception {
        this.messages = new ArrayList<Message>();
        for (int i = 0; i < count; i++) {
            final byte[] data = (strdata + i).getBytes();
            final Message msg = new Message(topic, data);
            final SendResult result = this.producer.sendMessage(msg);
            if (!result.isSuccess()) {
                throw new RuntimeException("Send message failed:" + result.getErrorMessage());
            }
            System.out.println(i);
            this.messages.add(msg);
        }
    }


    public void sendMessage(final int count, final byte[] data, final String topic) throws Exception {
        this.messages = new ArrayList<Message>();
        for (int i = 0; i < count; i++) {
            final Message msg = new Message(topic, data);
            final SendResult result = this.producer.sendMessage(msg);
            if (!result.isSuccess()) {
                throw new RuntimeException("Send message failed:" + result.getErrorMessage());
            }
            this.messages.add(msg);
        }
    }


    public void sendMessage2(final int count, final String strdata, final String topic) throws Exception {
        this.messages = new ArrayList<Message>();
        for (int i = 0; i < count; i++) {
            final byte[] data = strdata.getBytes();
            final Message msg = new Message(topic, data);
            final SendResult result = this.producer.sendMessage(msg);
            if (!result.isSuccess()) {
                throw new RuntimeException("Send message failed:" + result.getErrorMessage());
            }
            this.messages.add(msg);
        }
    }


    public void sendMessage_nProducer(final int count, final String strdata, final String topic, final int num)
            throws Exception {
        this.messages = new ArrayList<Message>();
        for (int j = 0; j < num; j++) {
            // 需要发布topic
            this.producerList.get(j).publish(topic);
            for (int i = 0; i < count; i++) {
                final byte[] data = ("hello" + j + i).getBytes();
                final Message msg = new Message(topic, data);
                final SendResult result = this.producerList.get(j).sendMessage(msg);
                if (!result.isSuccess()) {
                    throw new RuntimeException("Send message failed:" + result.getErrorMessage());
                }
                this.messages.add(msg);
            }
        }
    }


    public void localTxSendMessage_nProducer(final int count, final String strdata, final String topic, final int num)
            throws Exception {
        this.messages = new ArrayList<Message>();
        for (int j = 0; j < num; j++) {
            // 需要发布topic
            final MessageProducer messageProducer = this.producerList.get(j);
            messageProducer.publish(topic);
            for (int i = 0; i < count; i++) {
                final byte[] data = ("hello" + j + i).getBytes();
                final Message msg = new Message(topic, data);
                messageProducer.beginTransaction();
                final SendResult result = messageProducer.sendMessage(msg);
                if (!result.isSuccess()) {
                    messageProducer.rollback();
                    throw new RuntimeException("Send message failed:" + result.getErrorMessage());
                }
                messageProducer.commit();
                this.messages.add(msg);
            }
        }
    }


    public void sendMessage_nProducer_twoTopic(final int count, final String strdata, final String topic1,
            final String topic2, final int num, final Boolean attributed, final String attributed1,
            final String attributed2) throws Exception {
        this.messages = new ArrayList<Message>();
        for (int j = 0; j < num; j++) {
            // 需要发布topic
            this.producerList.get(j).publish(topic1);
            for (int i = 0; i < count; i++) {
                final byte[] data = ("hello" + j + i).getBytes();
                final Message msg;
                if (attributed == true) {
                    msg = new Message(topic1, data, attributed1);
                }
                else {
                    msg = new Message(topic1, data);
                }
                final SendResult result = this.producerList.get(j).sendMessage(msg);
                if (!result.isSuccess()) {
                    throw new RuntimeException("Send message failed:" + result.getErrorMessage());
                }
                this.messages.add(msg);
            }
        }
        for (int j = 0; j < num; j++) {
            // 需要发布topic
            this.producerList.get(j).publish(topic2);
            for (int i = 0; i < count; i++) {
                final byte[] data = ("hello" + i + j).getBytes();
                final Message msg;
                if (attributed == true) {
                    msg = new Message(topic1, data, attributed2);
                }
                else {
                    msg = new Message(topic1, data);
                }
                final SendResult result = this.producerList.get(j).sendMessage(msg);
                if (!result.isSuccess()) {
                    throw new RuntimeException("Send message failed:" + result.getErrorMessage());
                }
                this.messages.add(msg);
            }
        }
    }


    /** 订阅消息，并验证 */
    public void subscribe(final String topic, final int maxsize, final int count) throws Exception {
        this.subscribe(topic, maxsize, count, false);
    }


    /** 可重入的订阅消息，并验证 */
    public void subscribeRepeatable(final String topic, final int maxsize, final int count) throws Exception {
        this.subscribe(topic, maxsize, count, true);
    }


    private void subscribe(final String topic, final int maxsize, final int count, final boolean repeatable)
            throws Exception {
        // 订阅接收消息
        try {
            this.consumer.subscribe(topic, maxsize, new MessageListener() {

                public void recieveMessages(final Message messages) {
                    BaseMetaTest.this.queue.add(messages);
                }


                public Executor getExecutor() {
                    return null;
                }
            }).completeSubscribe();
        }
        catch (final MetaClientException e) {
            if (repeatable) {
                this.log.info("no need subscribe again");
            }
            else {
                throw e;
            }
        }
        while (this.queue.size() < count) {
            Thread.sleep(1000);
            System.out.println("等待接收消息" + count + "条，目前接收到" + this.queue.size() + "条");
        }

        // 检查消息是否接收到并校验内容
        assertEquals(count, this.queue.size());
        if (count != 0) {
            for (final Message msg : this.messages) {
                assertTrue(this.queue.contains(msg));
            }
        }
        this.log.info("received message count:" + this.queue.size());
    }


    public void subscribe_nConsumer(final String topic, final int maxsize, final int count, final int consumerNum,
            final int producerNum) throws Exception {
        this.consumerList = new ArrayList<MessageConsumer>();
        for (int i = 0; i < consumerNum; i++) {
            final ConcurrentLinkedQueue<Message> singlequeue = new ConcurrentLinkedQueue<Message>();
            this.consumerList.add(i, this.sessionFactory.createConsumer(new ConsumerConfig("group" + i)));
            this.consumerList.get(i).subscribe(topic, maxsize, new MessageListener() {

                public void recieveMessages(final Message messages) {
                    BaseMetaTest.this.queue.add(messages);
                    singlequeue.add(messages);
                }


                public Executor getExecutor() {
                    return null;
                }
            }).completeSubscribe();

            while (singlequeue.size() < count * producerNum) {
                Thread.sleep(1000);
                System.out.println("等待接收消息" + count * producerNum + "条，目前接收到" + singlequeue.size() + "条");
            }
            // 检查单个consumer消息是否接收到并校验内容
            assertEquals(count * producerNum, singlequeue.size());
            System.out.println(singlequeue.size());
            for (final Message msg : this.messages) {
                assertTrue(singlequeue.contains(msg));
            }
        }
        while (this.queue.size() < count * producerNum * consumerNum) {
            Thread.sleep(1000);
            System.out.println("等待接收消息count*num条，目前接收到" + this.queue.size() + "条");
        }
        // 检查全部消息是否接收到并校验内容
        assertEquals(count * producerNum * consumerNum, this.queue.size());
        System.out.println(this.queue.size());
        for (final Message msg : this.messages) {
            assertTrue(this.queue.contains(msg));
        }
    }


    public void subscribe_nConsumer_twoTopic(final String topic1, final String topic2, final int maxsize,
            final int count, final int consumerNum, final int producerNum) throws Exception {
        this.consumerList = new ArrayList<MessageConsumer>();
        for (int i = 0; i < consumerNum; i++) {
            final ConcurrentLinkedQueue<Message> singlequeue = new ConcurrentLinkedQueue<Message>();
            this.consumerList.add(i, this.sessionFactory.createConsumer(new ConsumerConfig("group" + i)));
            this.consumerList.get(i).subscribe(topic1, maxsize, new MessageListener() {

                public void recieveMessages(final Message messages) {
                    BaseMetaTest.this.queue.add(messages);
                    singlequeue.add(messages);
                }


                public Executor getExecutor() {
                    return null;
                }
            }).subscribe(topic2, maxsize, new MessageListener() {

                public void recieveMessages(final Message messages) {
                    BaseMetaTest.this.queue.add(messages);
                    singlequeue.add(messages);
                }


                public Executor getExecutor() {
                    return null;
                }
            }).completeSubscribe();

            while (singlequeue.size() < count * producerNum) {
                Thread.sleep(1000);
                System.out.println("等待接收消息count * producerNum条，目前接收到" + singlequeue.size() + "条");
            }
            // 检查单个consumer消息是否接收到并校验内容
            assertEquals(count * producerNum, singlequeue.size());
            System.out.println(singlequeue.size());
            for (final Message msg : this.messages) {
                assertTrue(singlequeue.contains(msg));
            }
        }
        while (this.queue.size() < count * producerNum * consumerNum) {
            Thread.sleep(1000);
            System.out.println("等待接收消息count * producerNum*consumerNum条，目前接收到" + this.queue.size() + "条");
        }
        // 检查全部消息是否接收到并校验内容
        assertEquals(count * producerNum * consumerNum, this.queue.size());
        System.out.println(this.queue.size());
        for (final Message msg : this.messages) {
            assertTrue(this.queue.contains(msg));
        }
    }
}
