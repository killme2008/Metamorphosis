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


/**
 *
 * @author 无花
 * @since 2011-5-27 上午11:39:00
 */

public class StatsResult {

    private boolean success;
    private String statsInfo;
    private final String serverUrl;//从哪个服务器接收
    private Exception e;
    public StatsResult(String serverUrl) {
        super();
        this.serverUrl = serverUrl;
    }
    public boolean isSuccess() {
        return success;
    }
    public void setSuccess(boolean success) {
        this.success = success;
    }
    public String getStatsInfo() {
        return statsInfo;
    }
    public void setStatsInfo(String statsInfo) {
        this.statsInfo = statsInfo;
    }
    public Exception getException() {
        return e;
    }
    public void setException(Exception e) {
        this.e = e;
    }
    public String getServerUrl() {
        return serverUrl;
    }
    
    
}