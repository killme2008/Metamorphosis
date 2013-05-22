package com.taobao.metamorphosis.client.extension.spring;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;


/**
 * Default message listener.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 * @param <T>
 */
public abstract class DefaultMessageListener<T> implements MessageListener,
org.springframework.beans.factory.InitializingBean, DisposableBean {
    private MessageBodyConverter<?> messageBodyConverter;
    static final Log log = LogFactory.getLog(DefaultMessageListener.class);
    private int processThreads = -1;

    private ExecutorService executor;


    protected void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }


    /**
     * Returns the threads number for processing messages.
     * 
     * @return
     */
    public int getProcessThreads() {
        return this.processThreads;
    }


    public MessageBodyConverter<?> getMessageBodyConverter() {
        return this.messageBodyConverter;
    }


    public void setMessageBodyConverter(MessageBodyConverter<?> messageBodyConverter) {
        this.messageBodyConverter = messageBodyConverter;
    }


    /**
     * Set the threads number for processing messages.
     * 
     * @param processThreads
     */
    public void setProcessThreads(int processThreads) {
        if (processThreads < 0) {
            throw new IllegalArgumentException("Invalid processThreads value:" + processThreads);
        }
        this.processThreads = processThreads;
    }


    @Override
    public void recieveMessages(Message message) throws InterruptedException {
        if (this.messageBodyConverter != null) {
            try {
                T body = (T) this.messageBodyConverter.fromByteArray(message.getData());
                this.onReceiveMessages(new MetaqMessage<T>(message, body));
            }
            catch (Exception e) {
                log.error("Convert message body from byte array failed,msg id is " + message.getId() + " and topic is "
                        + message.getTopic(), e);
                message.setRollbackOnly();
            }
        }
        else {
            this.onReceiveMessages(new MetaqMessage<T>(message, null));
        }
    }


    public abstract void onReceiveMessages(MetaqMessage<T> msg);


    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.processThreads > 0) {
            this.executor = Executors.newFixedThreadPool(this.processThreads);
        }
    }


    @Override
    public void destroy() throws Exception {
        if (this.executor != null) {
            this.executor.shutdown();
            this.executor = null;
        }
    }


    @Override
    public Executor getExecutor() {
        return this.executor;
    }

}
