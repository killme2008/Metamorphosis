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

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.CPULoadUtil;
import com.taobao.metamorphosis.tools.utils.MonitorResult;


/**
 * 
 * @author 无花
 * @since 2011-9-28 上午11:19:05
 */

public class CPULoadProber extends SystemProber {

    public CPULoadProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        if (monitorResult.getValue().intValue() > this.getMonitorConfig().getCpuLoadThreshold()) {
            this.alert(monitorResult.getIp() + " load 已经到达 " + monitorResult.getValue());
        }
    }


    @Override
    protected MonitorResult getMonitorResult(final MsgSender sender) throws Exception {
        return CPULoadUtil.getCpuLoad(sender.getHost(), CPULoadProber.this.getMonitorConfig().getLoginUser(),
            CPULoadProber.this.getMonitorConfig().getLoginPassword());
    }

}