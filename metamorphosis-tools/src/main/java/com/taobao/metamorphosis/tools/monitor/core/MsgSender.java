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
package com.taobao.metamorphosis.tools.monitor.core;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.exception.MetaOpeartionTimeoutException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.StatsCommand;


/**
 * @author ÎÞ»¨
 */

public class MsgSender {

    final static private String topicPrefix = "meta-monitor-topic-";

    final private MessageProducer producer;
    private final RemotingClientWrapper remotingClient;
    final private String topic;

    final private String serverUrl;
    final private URI url;

    private final byte[] data = "monitorTestMsgData".getBytes(Charset.forName("UTF-8"));

    private final MetaMessageSessionFactory metaMessageSessionFactory;


    public MsgSender(String serverUrl, String topicString, MonitorConfig monitorConfig) throws MetaClientException {
        this.topic = topicPrefix + topicString;
        this.serverUrl = serverUrl;
        try {
            this.url = new URI(serverUrl);
        }
        catch (URISyntaxException e) {
            throw new MetaClientException(e);
        }
        this.metaMessageSessionFactory = new MetaMessageSessionFactory(monitorConfig.metaClientConfigOf(serverUrl));
        this.remotingClient = this.metaMessageSessionFactory.getRemotingClient();
        this.producer = this.metaMessageSessionFactory.createProducer();
    }


    public String getTopic() {
        return this.topic;
    }


    public void publish() {
        this.producer.publish(this.topic);
    }


    public void dispose() {
        try {
            this.metaMessageSessionFactory.shutdown();
        }
        catch (MetaClientException e) {
            // ignore
        }
    }


    public SendResultWrapper sendMessage(long timeout) {
        SendResultWrapper result = new SendResultWrapper();
        Message message = this.nextMessage();
        result.setMessage(message);
        try {
            SendResult sendResult = this.producer.sendMessage(message, timeout, TimeUnit.MILLISECONDS);
            result.setSendResult(sendResult);
        }
        catch (MetaClientException e) {
            result.setException(e);
        }
        catch (InterruptedException e) {
            result.setException(e);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            result.setException(e);
        }
        return result;
    }


    public StatsResult getStats(String item, long timeout) {
        StatsResult statsResult = new StatsResult(this.serverUrl);
        try {
            BooleanCommand resp =
                    (BooleanCommand) this.remotingClient.invokeToGroup(this.serverUrl,
                        new StatsCommand(OpaqueGenerator.getNextOpaque(), item), timeout, TimeUnit.MILLISECONDS);

            final String resultStr = resp.getErrorMsg();

            switch (resp.getCode()) {
            case HttpStatus.Success: {
                statsResult.setSuccess(true);
                statsResult.setStatsInfo(resultStr);
                break;
            }
            default:
                statsResult.setSuccess(false);
            }
        }
        catch (TimeoutException e) {
            statsResult
                .setException(new MetaOpeartionTimeoutException("Send message timeout in " + timeout + " mills"));
        }
        catch (InterruptedException e) {
            // ignore
            statsResult.setException(e);
        }
        catch (Exception e) {
            statsResult.setException(new MetaClientException("send stats failed", e));
        }
        return statsResult;
    }


    private Message nextMessage() {
        return new Message(this.topic, this.data, String.valueOf(System.currentTimeMillis()));
    }


    public String getServerUrl() {
        return this.serverUrl;
    }


    public String getHost() {
        return this.url.getHost();
    }


    public int getPort() {
        int port = this.url.getPort();
        return port != -1 ? port : 8123;
    }

}