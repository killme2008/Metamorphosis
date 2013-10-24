package com.taobao.metamorphosis.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.sun.org.apache.xerces.internal.util.URI;
import com.taobao.metamorphosis.network.RemotingUtils;


public class RemotingClientWrapperUnitTest {

    @Test
    public void testTryGetLoopbackURL() throws Exception {

        RemotingUtils.setLocalHost("192.168.1.100");
        try {
            String url = "meta://192.168.1.100:8123";
            String loopbackURL = "meta://localhost:8123";
            assertEquals(loopbackURL, RemotingClientWrapper.tryGetLoopbackURL(url));
            assertEquals("localhost", new URI(loopbackURL).getHost());
        }
        finally {
            RemotingUtils.setLocalHost(null);
        }
    }
}
