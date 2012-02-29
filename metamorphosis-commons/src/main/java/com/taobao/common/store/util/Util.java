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
 *   yxq1871 (yuexuqiang at gmail.com)
 */
package com.taobao.common.store.util;

/**
 * store4j使用的util类
 * 
 * @author yxq1871 (yuexuqiang at gmail.com)
 * 
 */
public class Util {
    /**
     * 将一个对象注册给MBeanServer
     * 
     * @param o
     */
    public static void registMBean(final Object o, final String name) {
        MyMBeanServer.getInstance().registMBean(o, name);
    }
}