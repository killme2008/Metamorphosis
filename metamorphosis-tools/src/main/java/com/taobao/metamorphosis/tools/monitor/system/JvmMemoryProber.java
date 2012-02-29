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
import com.taobao.metamorphosis.tools.utils.JvmMemoryUtil;
import com.taobao.metamorphosis.tools.utils.MonitorResult;


/**
 * 
 * @author 无花
 * @since 2011-9-28 下午5:33:00
 */

public class JvmMemoryProber extends SystemProber {

    public JvmMemoryProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) {
        try {
            return JvmMemoryUtil.getMemoryInfo(sender.getHost(), this.getMonitorConfig().getJmxPort());
        }
        catch (Throwable e) {
            this.logger.error(e);
            return null;
        }
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        if (monitorResult.getValue() > 80) {
            this.alert(monitorResult.getIp() + "JVM 内存使用已经到达百分之 " + monitorResult.getValue());
        }
    }


    public static void main(String[] args) throws Exception {
        MonitorResult result = JvmMemoryUtil.getMemoryInfo("10.232.102.184", 9999);
        System.out.println(result.getValue());
    }

}