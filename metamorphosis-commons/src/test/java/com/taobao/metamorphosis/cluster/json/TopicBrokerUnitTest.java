package com.taobao.metamorphosis.cluster.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.Map;

import org.junit.Test;

import com.taobao.metamorphosis.utils.JSONUtils;


public class TopicBrokerUnitTest {

    @Test
    public void testToJsonParse() throws Exception {
        TopicBroker tb = new TopicBroker(10, "0-m");

        String json = tb.toJson();
        assertEquals(JSONUtils.deserializeObject("{\"numParts\":10,\"broker\":\"0-m\"}", Map.class),
            JSONUtils.deserializeObject(json, Map.class));
        System.out.println(json);

        TopicBroker parsed = TopicBroker.parse(json);
        assertNotSame(tb, parsed);
        assertEquals(tb, parsed);

    }
}
