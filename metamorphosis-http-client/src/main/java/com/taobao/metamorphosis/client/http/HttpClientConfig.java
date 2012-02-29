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

import javax.sql.DataSource;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;


public class HttpClientConfig extends ConsumerConfig {
    private static final String DEFAULT_GROUP = "httpgroup";
    private String serverURL;
    private int port;
    private String host;

    private int connectTimeout = 3000; // default to be 3 seconds
    private int timeout = 30000; // default to be 30 seconds
    private int retryTimes = 5; // default to be 5
    private int delayMillSeconds = 0;

    private int maxHostConnections = 20;
    private boolean connectionStaleCheckingEnabled = true;
    private int maxTotalConnections = 20;
    private int pollingIntervalTime = 15;


    public HttpClientConfig() {
        super();
        this.setGroup(DEFAULT_GROUP);
    }


    public HttpClientConfig(final String consumerId, final String group) {
        super(consumerId, group);
    }

    private DataSource dataSource;


    public int getPollingIntervalTime() {
        return this.pollingIntervalTime;
    }


    public void setPollingIntervalTime(final int pollingIntervalTime) {
        this.pollingIntervalTime = pollingIntervalTime;
    }


    public HttpClientConfig(final String serverURL) {
        this.serverURL = serverURL;
    }


    public HttpClientConfig(final String host, final int port) {
        this.host = host;
        this.port = port;
        this.setGroup(DEFAULT_GROUP);
    }


    public int getMaxHostConnections() {
        return this.maxHostConnections;
    }


    public void setMaxHostConnections(final int maxHostConnections) {
        this.maxHostConnections = maxHostConnections;
    }


    public boolean isConnectionStaleCheckingEnabled() {
        return this.connectionStaleCheckingEnabled;
    }


    public void setConnectionStaleCheckingEnabled(final boolean connectionStaleCheckingEnabled) {
        this.connectionStaleCheckingEnabled = connectionStaleCheckingEnabled;
    }


    public int getMaxTotalConnections() {
        return this.maxTotalConnections;
    }


    public void setMaxTotalConnections(final int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }


    public String getServerURL() {
        return this.serverURL;
    }


    public void setServerURL(final String serverURL) {
        this.serverURL = serverURL;
    }


    public int getConnectTimeout() {
        return this.connectTimeout;
    }


    public void setConnectTimeout(final int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }


    public int getDelayMillSeconds() {
        return this.delayMillSeconds;
    }


    public void setDelayMillSeconds(final int delayMillSeconds) {
        this.delayMillSeconds = delayMillSeconds;
    }


    public int getTimeout() {
        return this.timeout;
    }


    public void setTimeout(final int timeout) {
        this.timeout = timeout;
    }


    public int getRetryTimes() {
        return this.retryTimes;
    }


    public void setRetryTimes(final int retryTimes) {
        this.retryTimes = retryTimes;
    }


    public int getPort() {
        return this.port;
    }


    public void setPort(final int port) {
        this.port = port;
    }


    public String getHost() {
        return this.host;
    }


    public void setHost(final String host) {
        this.host = host;
    }


    public DataSource getDataSource() {
        return this.dataSource;
    }


    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

}