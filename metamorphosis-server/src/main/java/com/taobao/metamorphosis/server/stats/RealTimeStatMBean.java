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
package com.taobao.metamorphosis.server.stats;

import java.util.List;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-9-16 下午12:00:27
 */

public interface RealTimeStatMBean {

    /**
     * 查看实时统计的key信息
     * 
     * @return
     */
    public  List<String> getRealTimeStatItemNames();


    /**
     * 重新开始实时统计
     */
    public  void resetStat();


    /**
     * 实时统计进行的时间，单位秒
     * 
     * @return
     */
    public  long getStatDuration();


    /**
     * 获取实时统计结果
     * 
     * @param key1
     * @param key2
     * @param key3
     * @return
     */
    public  String getStatResult(String key1, String key2, String key3);


    public  String getStatResult(String key1, String key2);


    public  String getStatResult(String key1);


    public String getGroupedRealTimeStatResult(String key1);


    public long getDuration();
}