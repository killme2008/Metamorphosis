package com.taobao.metamorphosis.client.extension.spring;

import java.io.IOException;
import java.io.Serializable;

import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.codec.impl.JavaSerializer;


/**
 * Message body converter using java serialization.
 * 
 * @author dennis<killme2008@gmai.com>
 * @since 1.4.5
 * 
 */
public class JavaSerializationMessageBodyConverter implements MessageBodyConverter<Serializable> {
    JavaSerializer serializer = new JavaSerializer();


    @Override
    public byte[] convertBody(Serializable body) throws MetaClientException {
        try {
            return this.serializer.encodeObject(body);
        }
        catch (IOException e) {
            throw new MetaClientException(e);

        }
    }

}
