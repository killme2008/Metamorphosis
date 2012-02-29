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
package com.taobao.metamorphosis.tools.monitor.msgprobe;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.alert.Alarm;
import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MonitorConfig;


/**
 * @author 无花
 */

@Deprecated
public class SendReceiveMonitor {
    private static Logger logger = Logger.getLogger(SendReceiveMonitor.class);


    public static void main(String[] args) {

        MsgProber prober = null;
        MonitorConfig monitorConfig = new MonitorConfig();
        try {
            monitorConfig.loadInis("monitor.properties");
            CoreManager coreManager = CoreManager.getInstance(monitorConfig, 1);
            prober = new MsgProber(coreManager);
            prober.init();
        }
        catch (Throwable e) {
            logger.error("fail to startup", e);
            System.exit(-1);
        }

        try {
            prober.prob();
        }
        catch (Throwable e) {
            logger.error("监控系统意外终止", e);
            Alarm.alert("监控系统意外终止", monitorConfig);
        }

    }

}