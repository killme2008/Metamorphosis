package com.taobao.metamorphosis.client.extension.spring;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Default message listener.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 * @param <T>
 */
public abstract class DefaultMessageListener<T> implements MessageListener {
    private MessageBodyConverter<T> messageBodyConverter;
    static final Log log = LogFactory.getLog(DefaultMessageListener.class);
    private int processThreads = -1;

    private Executor executor;


    /**
     * Returns the threads number for processing messages.
     * 
     * @return
     */
    public int getProcessThreads() {
        return this.processThreads;
    }


    /**
     * Set the threads number for processing messages.
     * 
     * @param processThreads
     */
    public void setProcessThreads(int processThreads) {
        this.processThreads = processThreads;
    }


    @Override
    public void recieveMessages(Message message) throws InterruptedException {
        if (this.messageBodyConverter != null) {
            try {
                T body = this.messageBodyConverter.fromByteArray(message.getData());
                this.onReceiveMessages(new MetaqMessage<T>(message, body));
            }
            catch (MetaClientException e) {
                log.error("Convert message body from byte array failed", e);
                message.setRollbackOnly();
            }
        }
        else {
            this.onReceiveMessages(new MetaqMessage<T>(message, null));
        }
    }


    public abstract void onReceiveMessages(MetaqMessage<T> msg);


    @PostConstruct
    public void init() {
        if (this.processThreads > 0) {
            this.executor = Executors.newFixedThreadPool(this.processThreads);
        }
    }


    @Override
    public Executor getExecutor() {
        return this.executor;
    }

}
