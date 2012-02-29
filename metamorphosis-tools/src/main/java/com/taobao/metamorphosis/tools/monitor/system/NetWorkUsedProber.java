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
package com.taobao.metamorphosis.tools.monitor.system;

import java.util.Map;

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.MonitorResult;
import com.taobao.metamorphosis.tools.utils.NetWorkUtil;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç4:22:27
 */

public class NetWorkUsedProber extends SystemProber {

    public NetWorkUsedProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) {
        Map<String, MonitorResult> result =
                NetWorkUtil.newInstance().getNetWorkUsed(sender.getHost(), this.getMonitorConfig().getLoginUser(),
                    this.getMonitorConfig().getLoginPassword());
        return new MonitorResultWrapper(result);
    }


    @Override
    protected void processResult0(MonitorResult monitorResult) {
        Map<String, MonitorResult> resultMap = ((MonitorResultWrapper) monitorResult).resultMap;
        if (resultMap == null) {
            return;
        }

        for (MonitorResult each : resultMap.values()) {
            this.processResultHook.handler(each);
        }
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        // do nothing
    }

    private static class MonitorResultWrapper extends MonitorResult {
        Map<String, MonitorResult> resultMap;


        MonitorResultWrapper(Map<String, MonitorResult> resultMap) {
            this.resultMap = resultMap;
        }
    }

}