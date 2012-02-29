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

/**
 * 
 * @author 无花
 * @since 2011-9-28 下午1:47:30
 */

public class ConsoleConstant {
    public static final String CPU_LOAD = "cpu load";
    public static final String CPU_LOAD_CMD = "uptime";
    public static final String CPU_LOAD_NAME = "cpuLoad";
    public static final String MEM_NAME = "memory";
    public static final String DISK_NAME = "disk";
    public static final String RX_NETWORK_NAME = "rxNetwork";
    public static final String TX_NETWORK_NAME = "txNetwork";

    public static final String DISK = "disk used";
    public static final String DISK_CMD = "df -h | grep home";

    public static final String NETWORK_CMD = "/sbin/ifconfig eth0 | grep bytes";
    public static final String RX_NETWORK = "RX network used";
    public static final String TX_NETWORK = "TX network used";

    public static final String MEM = "memory used";

    public static final String DUMMY = "dummy";

    private static final String CONN_FORMAT =
            "netstat -an| grep :%s | grep -v 0.0.0.0 |awk '{ print $5 }' | sort|awk -F: '{print $1}'| uniq -c|sort -r| awk '{print $1,$2}'";


    /** 获得命令字符串:分组统计连接到指定端口的客户端ip连接数 */
    public static String getConnOfPortCMD(int port) {
        return String.format(CONN_FORMAT, port);
    }


    public static void main(String[] args) {
        System.out.println(getConnOfPortCMD(8123));
    }
}