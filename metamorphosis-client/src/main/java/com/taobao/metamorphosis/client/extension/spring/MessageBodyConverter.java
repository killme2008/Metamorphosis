package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Messge body object converter.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * @param <T>
 */
public interface MessageBodyConverter<T> {
    public byte[] convertBody(T body) throws MetaClientException;
}
