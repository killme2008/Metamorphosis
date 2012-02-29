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
import java.util.HashMap;
import java.util.Map;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç4:00:27
 */

public class NetWorkUtil {
    private NetWorkUtil() {
    }


    public static NetWorkUtil newInstance() {
        return new NetWorkUtil();
    }


    public Map<String, MonitorResult> getNetWorkUsed(String ip, String user, String password) {
        SSHSupport support = SSHSupport.newInstance(user, password, ip);
        String result = support.execute(ConsoleConstant.NETWORK_CMD);
        if (result == null) {
            return null;
        }
        String[] subStrings = result.split(":");
        if (subStrings.length != 3) {
            return null;
        }
        Map<String, MonitorResult> resultMap = new HashMap<String, MonitorResult>();

        MonitorResult oneResult = new MonitorResult();
        oneResult.setDescribe("");
        oneResult.setIp(ip);
        oneResult.setKey(ConsoleConstant.RX_NETWORK);
        oneResult.setTime(new Date());
        oneResult.setValue(this.getTXResult(subStrings[1]));
        resultMap.put("RX", oneResult);

        MonitorResult oneResult2 = new MonitorResult();
        oneResult2.setDescribe("");
        oneResult2.setIp(ip);
        oneResult2.setKey(ConsoleConstant.TX_NETWORK);
        oneResult2.setTime(new Date());
        oneResult2.setValue(this.getRxResult(subStrings[2]));
        resultMap.put("TX", oneResult2);
        return resultMap;
    }


    public double getTXResult(String result) {
        String sub = result.substring(0, result.indexOf("("));
        if (sub != null) {
            return Double.valueOf(sub.trim());
        }

        return 0;
    }


    public double getRxResult(String result) {
        String sub = result.substring(0, result.indexOf("("));
        if (sub == null) {
            return 0;
        }
        return Double.valueOf(sub.trim());
    }


    public static void main(String[] args) {
        System.out.println(NetWorkUtil.newInstance().getNetWorkUsed("10.232.16.22", "notify", "tjjtds"));
    }
}