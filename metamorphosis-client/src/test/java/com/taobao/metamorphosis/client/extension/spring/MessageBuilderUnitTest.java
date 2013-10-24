package com.taobao.metamorphosis.client.extension.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.junit.Test;

import com.taobao.metamorphosis.Message;


public class MessageBuilderUnitTest {

    public static class MyTest implements Serializable {
        private long value = 1000L;


        public long getValue() {
            return this.value;
        }


        public void setValue(long value) {
            this.value = value;
        }

    }


    @Test
    public void testBuildMessageWithBodyObject() throws Exception {
        MessageBuilder mb = MessageBuilder.withTopic("test");
        mb.withAttribute("a attribute").withBody(new MyTest());

        JavaSerializationMessageBodyConverter converter = new JavaSerializationMessageBodyConverter();
        Message msg = mb.build(converter);
        assertNotNull(msg);
        assertEquals("test", msg.getTopic());
        assertEquals("a attribute", msg.getAttribute());
        assertTrue(msg.hasAttribute());
        byte[] data = msg.getData();
        Object obj = converter.fromByteArray(data);
        assertTrue(obj instanceof MyTest);
        assertEquals(1000L, ((MyTest) obj).getValue());
    }


    @Test
    public void testBuildMessageWithPayload() throws Exception {
        MessageBuilder mb = MessageBuilder.withTopic("test");
        mb.withAttribute("a attribute").withPayload(new byte[128]);

        Message msg = mb.build();
        assertNotNull(msg);
        assertEquals("test", msg.getTopic());
        assertEquals("a attribute", msg.getAttribute());
        assertTrue(msg.hasAttribute());
        byte[] data = msg.getData();
        assertEquals(128, data.length);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testBuildMessageWithNothing() {
        MessageBuilder.withTopic("test").build();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testWithPayloadHasBody() {
        MessageBuilder.withTopic("test").withBody(new MyTest()).withPayload(new byte[128]);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testWithBodyHasPayload() {
        MessageBuilder.withTopic("test").withPayload(new byte[128]).withBody(new MyTest());
    }
}
