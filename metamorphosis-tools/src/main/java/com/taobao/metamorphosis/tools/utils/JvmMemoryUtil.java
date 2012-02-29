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
package com.taobao.metamorphosis.tools.utils;

import java.util.Date;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç5:02:13
 */

public class JvmMemoryUtil {

    public static MonitorResult getMemoryInfo(String ip, int port) throws Exception {
        JMXClient jmxClient = JMXClient.getJMXClient(ip, port);
        ObjectName objectName = new ObjectName("java.lang:type=Memory");
        CompositeData memoryInfo = (CompositeData) jmxClient.getAttribute(objectName, "HeapMemoryUsage");
        jmxClient.close();
        double max = Double.valueOf(memoryInfo.get("max").toString());
        double used = Double.valueOf(memoryInfo.get("used").toString());
        double usedPercent = (used / max) * 100;
        MonitorResult oneResult = new MonitorResult();
        oneResult.setDescribe("");
        oneResult.setKey(ConsoleConstant.MEM);
        oneResult.setIp(ip);
        oneResult.setTime(new Date());
        oneResult.setValue(usedPercent);
        return oneResult;
    }
}