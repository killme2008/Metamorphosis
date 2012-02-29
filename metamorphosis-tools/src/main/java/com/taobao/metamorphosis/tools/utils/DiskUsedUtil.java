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


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç3:21:25
 */

public class DiskUsedUtil {

    private DiskUsedUtil() {
    }


    public static MonitorResult getDiskUsed(String ip, String user, String password) {
        SSHSupport support = SSHSupport.newInstance(user, password, ip);
        String result = support.execute(ConsoleConstant.DISK_CMD);
        double used = getDiskUsed(result);
        MonitorResult oneResult = new MonitorResult();
        oneResult.setDescribe("");
        oneResult.setKey(ConsoleConstant.DISK);
        oneResult.setIp(ip);
        oneResult.setTime(new Date());
        oneResult.setValue(used);

        return oneResult;
    }


    private static double getDiskUsed(String loadStr) {
        if (loadStr == null || loadStr.indexOf("%") == -1) {
            return 0.00;
        }
        int index = loadStr.indexOf("%");
        String subStr = loadStr.substring(index - 3, index).trim();

        if (subStr != null) {
            return Double.valueOf(subStr);
        }
        return 0.00;
    }


    public static void main(String[] args) {
        System.out.println(DiskUsedUtil.getDiskUsed("/dev/sda9              35G  9.7G   24G  90% /home"));
    }
}