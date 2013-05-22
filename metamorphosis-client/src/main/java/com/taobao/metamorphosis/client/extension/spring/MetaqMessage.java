package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * a MetaQ message wrapper with message body object.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class MetaqMessage<T> {

    private final Message rawMessage;

    private final T body;


    public MetaqMessage(Message rawMessage, T body) {
        super();
        if (rawMessage == null) {
            throw new IllegalArgumentException("Null message");
        }
        this.rawMessage = rawMessage;
        this.body = body;
    }


    /**
     * Returns the raw metaq message.
     * 
     * @return
     */
    public Message getRawMessage() {
        return this.rawMessage;
    }


    public void setRollbackOnly() {
        this.rawMessage.setRollbackOnly();
    }


    public boolean isReadOnly() {
        return this.rawMessage.isReadOnly();
    }


    public void setReadOnly(boolean readOnly) {
        this.rawMessage.setReadOnly(readOnly);
    }


    public boolean hasAttribute() {
        return this.rawMessage.hasAttribute();
    }


    public long getId() {
        return this.rawMessage.getId();
    }


    public String getAttribute() {
        return this.rawMessage.getAttribute();
    }


    public String getTopic() {
        return this.rawMessage.getTopic();
    }


    public byte[] getData() {
        return this.rawMessage.getData();
    }


    public Partition getPartition() {
        return this.rawMessage.getPartition();
    }


    /**
     * Returns the message body object.It's converted by message body converter.
     * 
     * @return
     */
    public T getBody() {
        return this.body;
    }

}
