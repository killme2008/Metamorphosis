package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.metamorphosis.Message;


/**
 * a MetaQ message.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class MetaqMessage<T> {

    private final Message rawMessage;

    private final T body;


    public MetaqMessage(Message rawMessage, T body) {
        super();
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


    /**
     * Returns the message body object.It's converted by message body converter.
     * 
     * @return
     */
    public T getBody() {
        return this.body;
    }

}
