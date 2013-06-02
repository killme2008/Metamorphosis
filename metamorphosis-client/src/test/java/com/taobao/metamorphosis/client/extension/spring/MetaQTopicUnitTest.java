package com.taobao.metamorphosis.client.extension.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;


public class MetaQTopicUnitTest {

    @Test
    public void testAsMapKey() {
        MetaqTopic metaQTopic1 = new MetaqTopic("test", 1024, new ConsumerConfig("test-group"));
        MetaqTopic metaQTopic2 = new MetaqTopic("test", 1024, new ConsumerConfig("test-group"));
        Map<MetaqTopic, Boolean> map = new HashMap<MetaqTopic, Boolean>();
        assertEquals(metaQTopic1, metaQTopic2);
        map.put(metaQTopic1, true);
        assertEquals("test", metaQTopic1.getTopic());
        assertEquals("test-group", metaQTopic1.getConsumerConfig().getGroup());
        assertEquals(1024, metaQTopic1.getMaxBufferSize());
        assertEquals(1, map.size());
        assertTrue(map.get(metaQTopic1));
        assertTrue(map.get(metaQTopic2));
        metaQTopic2.setAlwaysConsumeFromMaxOffset(true);
        assertNull(map.get(metaQTopic2));
        assertTrue(map.get(metaQTopic1));
    }
}
