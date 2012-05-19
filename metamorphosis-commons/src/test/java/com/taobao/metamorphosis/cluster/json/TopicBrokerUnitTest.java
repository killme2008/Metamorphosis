package com.taobao.metamorphosis.cluster.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;


public class TopicBrokerUnitTest {

    @Test
    public void testToJsonParse() throws Exception {
        TopicBroker tb = new TopicBroker(10, "0-m");

        String json = tb.toJson();
        assertEquals("{\"numParts\":10,\"broker\":\"0-m\"}", json);
        System.out.println(json);

        TopicBroker parsed = TopicBroker.parse(json);
        assertNotSame(tb, parsed);
        assertEquals(tb, parsed);

    }
}
