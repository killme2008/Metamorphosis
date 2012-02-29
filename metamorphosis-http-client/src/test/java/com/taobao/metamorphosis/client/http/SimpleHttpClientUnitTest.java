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
package com.taobao.metamorphosis.client.http;

import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.logicalcobwebs.proxool.ProxoolDataSource;
import org.testng.annotations.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageIterator;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


@Ignore
public class SimpleHttpClientUnitTest {
    private final static Log logger = LogFactory.getLog(SimpleHttpClientUnitTest.class);

    private final long offset = 0;


    @Test
    public void testSyncConsume() throws MetaClientException, InterruptedException {
        final SimpleHttpConsumer client = new SimpleHttpConsumer(new HttpClientConfig("localhost", 8080));

        MessageIterator it = null;
        // get messages
        long syncOffSet = this.offset;
        while ((it = client.get("meta-test", new Partition(1, 1), syncOffSet, 99999)) != null) {
            while (it.hasNext()) {
                final Message msg = it.next();
                logger.info("Receive message:" + new String(msg.getData()));
                logger.info("message attribute:" + msg.getAttribute());
            }

            syncOffSet += it.getOffset();
        }

    }


    @Test
    public void testSendMessage() throws MetaClientException {
        final SimpleHttpProducer client = new SimpleHttpProducer(new HttpClientConfig("localhost", 8080));
        final Message message = new Message("meta-test", "world".getBytes());
        message.setAttribute(System.currentTimeMillis() + "");
        final SendResult result = client.sendMessage(message, new Partition(1, 1));
        logger.info("send message:" + result.isSuccess());
    }


    /**
     * Use main because it will quit after main flow of test finish running if
     * using test case.
     * 
     * @param args
     * @throws MetaClientException
     */
    public final static void main(final String[] args) throws MetaClientException {
        final HttpClientConfig httpClientConfig = new HttpClientConfig("localhost", 8080);

        final ProxoolDataSource dataSource = new ProxoolDataSource();
        dataSource.setDriver("com.mysql.jdbc.Driver");
        dataSource.setDriverUrl("jdbc:mysql://localhost:3306/meta");
        dataSource.setUser("root");
        dataSource.setPassword("1234QWER");
        httpClientConfig.setDataSource(dataSource);
        final SimpleHttpConsumer client = new SimpleHttpConsumer(httpClientConfig);
        client.subscribe("meta-test", new Partition(1, 1), 99999, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                logger.info(new String(message.getData()));
            }


            @Override
            public Executor getExecutor() {
                return null;
            }
        });

        client.completeSubscribe();
    }
}