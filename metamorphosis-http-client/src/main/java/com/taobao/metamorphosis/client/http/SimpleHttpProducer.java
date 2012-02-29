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

import java.util.regex.Pattern;

import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.client.producer.SimpleMessageProducer;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.utils.MessageFlagUtils;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


public class SimpleHttpProducer extends SimpleHttpClient {
    private static final Pattern RESULT_SPLITER = Pattern.compile(" ");


    public SimpleHttpProducer(final HttpClientConfig config) {
        super(config);
    }


    /**
     * 发送消息
     * 
     * @param message
     * @throws MetaClientException
     */
    public SendResult sendMessage(final Message message, final Partition partition) throws MetaClientException {
        final long start = System.currentTimeMillis();
        this.checkMessage(message);
        final int flag = MessageFlagUtils.getFlag(message);
        final byte[] data = SimpleMessageProducer.encodeData(message);
        final String uri =
                "/put/" + partition.getBrokerId() + "?topic=" + message.getTopic() + "&partition="
                        + partition.getPartition() + "&flag=" + flag + "&length=" + data.length;
        final PostMethod postMethod = new PostMethod(uri);
        try {
            postMethod.setRequestEntity(new ByteArrayRequestEntity(data));
            this.httpclient.executeMethod(postMethod);
            final String resultStr = postMethod.getResponseBodyAsString();
            switch (postMethod.getStatusCode()) {
            case HttpStatus.Success: {
                // messageId partition offset
                final String[] tmps = RESULT_SPLITER.split(resultStr);
                // 成功，设置消息id，消息id由服务器产生
                MessageAccessor.setId(message, Long.parseLong(tmps[0]));
                return new SendResult(true, new Partition(0, Integer.parseInt(tmps[1])), Long.parseLong(tmps[2]), null);
            }
            case HttpStatus.Forbidden: {
                // 服务器分区关闭了,先写本地
                return new SendResult(false, null, -1, String.valueOf(HttpStatus.Forbidden));
            }
            default:
                return new SendResult(false, null, -1, resultStr);
            }
        }
        catch (final Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new MetaClientException(e);
        }
        finally {
            final long duration = System.currentTimeMillis() - start;
            System.out.println(duration);
            MetaStatLog.addStatValue2(null, StatConstants.PUT_TIME_STAT, message.getTopic(), duration);
            postMethod.releaseConnection();
        }

    }


    private void checkMessage(final Message message) throws MetaClientException {
        if (message == null) {
            throw new MetaClientException("Null message");
        }
        if (StringUtils.isBlank(message.getTopic())) {
            throw new MetaClientException("Blank topic");
        }
        if (message.getData() == null) {
            throw new MetaClientException("Null data");
        }
    }

}