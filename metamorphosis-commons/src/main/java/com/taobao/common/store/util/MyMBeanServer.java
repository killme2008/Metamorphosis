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
 *   dogun (yuexuqiang at gmail.com)
 */
package com.taobao.common.store.util;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;


/**
 * 便于注册MBean的工具类
 * 
 * 只提供注册MBean，不对外暴露JMXConnectorServer
 * 
 * @author dogun (yuexuqiang at gmail.com)
 * 
 */
public final class MyMBeanServer {
    private final Log log = LogFactory.getLog(MyMBeanServer.class);
    private MBeanServer mbs = null;

    private static MyMBeanServer me = new MyMBeanServer();


    private MyMBeanServer() {
        try {
            final boolean useJmx = Boolean.parseBoolean(System.getProperty("store4j.useJMX", "false"));
            if (useJmx) {
                mbs = ManagementFactory.getPlatformMBeanServer();
            }
        }
        catch (final Exception e) {
            log.error("create MBServer error", e);
        }
    }


    /**
     * 获得MBeanServer
     * 
     * @return MyMBeanServer
     */
    public static MyMBeanServer getInstance() {
        return me;
    }


    /**
     * 注册一个MBean
     * 
     * @param o
     * @param name
     */
    public void registMBean(final Object o, final String name) {
        // 注册MBean
        if (null != mbs) {
            try {
                mbs.registerMBean(o, new ObjectName(o.getClass().getPackage().getName() + ":type="
                        + o.getClass().getSimpleName()
                        + (null == name ? (",id=" + o.hashCode()) : (",name=" + name + "-" + o.hashCode()))));
            }
            catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}