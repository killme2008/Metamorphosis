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

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.client.Shutdownable;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Based on http-client.jar library thread safe using httpclient connection pool
 */
public class SimpleHttpClient implements Shutdownable {
    protected Log logger = LogFactory.getLog(this.getClass());
    protected HttpClient httpclient = null;
    private MultiThreadedHttpConnectionManager connectionManager;


    public SimpleHttpClient(HttpClientConfig config) {
        initHttpClient(config);
    }


    protected void initHttpClient(HttpClientConfig config) {
        HostConfiguration hostConfiguration = new HostConfiguration();
        hostConfiguration.setHost(config.getHost(), config.getPort());

        connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.closeIdleConnections(config.getPollingIntervalTime() * 4000);

        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setStaleCheckingEnabled(config.isConnectionStaleCheckingEnabled());
        params.setMaxConnectionsPerHost(hostConfiguration, config.getMaxHostConnections());
        params.setMaxTotalConnections(config.getMaxTotalConnections());
        params.setConnectionTimeout(config.getTimeout());
        params.setSoTimeout(60 * 1000);

        connectionManager.setParams(params);
        httpclient = new HttpClient(connectionManager);
        httpclient.setHostConfiguration(hostConfiguration);
    }


    @Override
    public void shutdown() throws MetaClientException {
        MultiThreadedHttpConnectionManager.shutdownAll();
    }

}