/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.http;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;


@Ignore
public class MetamorphosisOnJettyBrokerTest {

    private static MetamorphosisOnJettyBroker broker = null;
    private static final Log logger = LogFactory.getLog(MetamorphosisOnJettyBrokerTest.class);
    private final String message = "testMetaOnJetty";


    @BeforeClass
    public static void setUp() throws IOException {
        final MetaConfig metaConfig = new MetaConfig();
        metaConfig.setBrokerId(100);
        metaConfig.setHostName("localhost");
        metaConfig.setServerPort(8199);

        broker = new MetamorphosisOnJettyBroker();
        final MetaMorphosisBroker metaBroker = new MetaMorphosisBroker(metaConfig);
        final Properties properites = new Properties();
        properites.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("jettyBroker.properties"));
        broker.init(metaBroker, properites);
        broker.start();
    }


    @Test
    public void testPutMessage() {
        logger.info("produce消息：");
        final WebConversation wc = new WebConversation();
        try {
            final WebRequest request =
                    new PostMethodWebRequest("http://localhost:8080/put?topic=test&partition=1&offset=1");
            request.setParameter("data", com.meterware.httpunit.Base64.encode("hello"));
            final WebResponse response = wc.getResource(request);
            logger.info(response.getText());
        }
        catch (final Exception e) {
            logger.error(e.getMessage(), e);
            assert false;
        }
    }


    @Test
    public void testGetMessage() {
        logger.info("consume消息：");
        final WebConversation wc = new WebConversation();
        try {
            final WebResponse wr = wc.getResponse("http://localhost:8080/get?topic=test&partition=1&offset=1");
            assert wr.getText().equals(this.message);
        }
        catch (final Exception e) {
            logger.error(e);
            assert false;
        }
    }


    @Test
    public void testGetOffset() {
        logger.info("获得消息的offset：");
        final WebConversation wc = new WebConversation();
        try {
            final WebResponse wr = wc.getResponse("http://localhost:8080/offset?topic=test&partition=1&offset=1");
            logger.info(wr.getText());
        }
        catch (final Exception e) {
            logger.error(e);
            assert false;
        }
    }


    @AfterClass
    public static void tearDown() {
        broker.stop();
    }

}