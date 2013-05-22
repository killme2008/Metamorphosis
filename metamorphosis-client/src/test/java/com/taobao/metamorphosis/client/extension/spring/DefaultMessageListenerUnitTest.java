package com.taobao.metamorphosis.client.extension.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.exception.MetaClientException;


public class DefaultMessageListenerUnitTest {

    private static class MyMessageListener extends DefaultMessageListener<String> {

        MetaqMessage<String> recvMsg;


        @Override
        public void onReceiveMessages(MetaqMessage<String> msg) {
            this.recvMsg = msg;

        }

    }


    @Test
    public void testOnReceiveMessagesWithConverter() throws Exception {
        MyMessageListener listener = new MyMessageListener();
        JavaSerializationMessageBodyConverter messageBodyConverter = new JavaSerializationMessageBodyConverter();
        listener.setMessageBodyConverter(messageBodyConverter);
        listener.afterPropertiesSet();
        Message message = new Message("test", messageBodyConverter.toByteArray("hello world"));
        listener.recieveMessages(message);

        assertNotNull(listener.recvMsg);
        assertEquals("hello world", listener.recvMsg.getBody());
        assertSame(message, listener.recvMsg.getRawMessage());

    }


    @Test
    public void testConvertMessageBodyFailure() throws Exception {
        MyMessageListener listener = new MyMessageListener();
        JavaSerializationMessageBodyConverter messageBodyConverter = new JavaSerializationMessageBodyConverter();
        listener.setMessageBodyConverter(new MessageBodyConverter<String>() {

            @Override
            public byte[] toByteArray(String body) throws MetaClientException {
                throw new RuntimeException();
            }


            @Override
            public String fromByteArray(byte[] bs) throws MetaClientException {
                throw new RuntimeException();
            }

        });
        listener.afterPropertiesSet();
        Message message = new Message("test", messageBodyConverter.toByteArray("hello world"));
        listener.recieveMessages(message);

        assertNull(listener.recvMsg);
        assertTrue(MessageAccessor.isRollbackOnly(message));
    }


    @Test
    public void testInitDestroy() throws Exception {
        MyMessageListener listener = new MyMessageListener();
        listener.setProcessThreads(10);
        assertNull(listener.getExecutor());
        listener.afterPropertiesSet();
        assertNotNull(listener.getExecutor());
        listener.destroy();
        assertNull(listener.getExecutor());
    }

    @Test
    public void testOnReceiveMessagesWithoutConverter() throws Exception {
        MyMessageListener listener = new MyMessageListener();
        JavaSerializationMessageBodyConverter messageBodyConverter = new JavaSerializationMessageBodyConverter();
        listener.afterPropertiesSet();
        Message message = new Message("test", messageBodyConverter.toByteArray("hello world"));
        listener.recieveMessages(message);

        assertNotNull(listener.recvMsg);
        assertNull(listener.recvMsg.getBody());
        assertSame(message, listener.recvMsg.getRawMessage());

    }
}
