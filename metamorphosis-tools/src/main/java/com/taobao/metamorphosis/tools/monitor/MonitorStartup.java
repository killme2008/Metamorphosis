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
package com.taobao.metamorphosis.tools.monitor;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.alert.Alarm;


/**
 * 监控启动器
 * 
 * @author 无花
 * @since 2011-5-30 上午11:21:08
 */

public class MonitorStartup {
    private static Logger logger = Logger.getLogger(MonitorStartup.class);


    public static void main(String[] args) {
        ProberManager proberManager = new ProberManager();

        try {
            String source = null;
            proberManager.initProbers(source);
        }
        catch (InitException e) {
            logger.error("fail to startup", e);
            System.exit(-1);
        }

        try {
            proberManager.startProb();
        }
        catch (Throwable e) {
            logger.error("监控系统意外终止", e);
            Alarm.alert("监控系统意外终止", proberManager.getMonitorConfig());
        }
    }

}