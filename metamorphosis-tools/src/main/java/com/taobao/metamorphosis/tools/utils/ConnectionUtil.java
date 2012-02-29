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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;


/**
 * 
 * @author 无花
 * @since 2011-9-29 上午9:56:05
 */

public class ConnectionUtil {

    public static MonitorResult getConnectedCount(String ip, final String toString, String user, String password) {
        SSHSupport support = SSHSupport.newInstance(user, password, ip);
        String result = support.execute("netstat -nat|grep " + toString + "|awk '{print $5}'");

        final AtomicInteger count = new AtomicInteger(0);
        try {
            com.taobao.metamorphosis.utils.Utils.processEachLine(result,
                new com.taobao.metamorphosis.utils.Utils.Action() {

                    @Override
                    public void process(String line) {
                        if (StringUtils.isNotBlank(line) && line.contains(toString)) {
                            count.incrementAndGet();
                        }

                    }
                });
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        MonitorResult monitorResult = new MonitorResult();
        monitorResult.setDescribe("");
        monitorResult.setKey(ConsoleConstant.DUMMY);
        monitorResult.setIp(ip);
        monitorResult.setTime(new Date());
        monitorResult.setValue(count.doubleValue());
        return monitorResult;
    }


    public static boolean IsConnected(String ip, final String toString, String user, String password) {
        return getConnectedCount(ip, toString, user, password).getValue().intValue() > 0;
    }


    /**
     * <pre>
     * 分组统计连接到指定端口的客户端ip连接数
     * @param ip
     * @param port
     * @param user
     * @param password
     * @return 
     * 3 10.232.101.45
     * 2 10.13.44.24
     * 1 10.32.100.151
     * 1 10.32.100.133
     * 1 10.32.100.131
     * 1 10.232.37.54
     * </pre>
     */
    public static String getConnectionInfo(String ip, int port, String user, String password) {
        SSHSupport support = SSHSupport.newInstance(user, password, ip);
        return support.execute(ConsoleConstant.getConnOfPortCMD(port));
    }


    public static void main(String[] args) {
        System.out.println(IsConnected("10.232.102.184", "10.232.102.188:2181", "wuhua", "Wuhua_123"));
        System.out.println(IsConnected("10.232.102.184", "2181", "wuhua", "Wuhua_123"));
        System.out.println(IsConnected("10.232.102.184", "10.232.102.189:2181", "wuhua", "Wuhua_123"));
        System.out.println(new AtomicInteger(0).doubleValue() > 0);
    }
}