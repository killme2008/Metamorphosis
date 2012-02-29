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
 *   wuhua <wq163@163.com> 
 */
package com.taobao.metamorphosis.metaslave;

import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.utils.MetaStatLog;


/**
 * 
 * @author 无花
 * @since 2011-11-9 上午10:35:17
 */

public class SlaveStatsManager {

    /**
     * slave服务端put
     */
    public static final String SLAVE_CMD_PUT = "put_slave";

    private final StatsManager statsManager;


    public SlaveStatsManager(final StatsManager statsManager) {
        this.statsManager = statsManager;
    }


    public void statsSlavePut(final String topic, final String partition, final int c) {
        this.statsManager.statsRealtimePut(c);
        MetaStatLog.addStatValue2(null, SLAVE_CMD_PUT, topic, partition, c);
    }


    public void statsMessageSize(final String topic, final int length) {
        this.statsManager.statsMessageSize(topic, length);
    }


    public void statsSlavePutFailed(final String topic, final String partition, final int c) {
        this.statsManager.statsPutFailed(topic, partition, c);
    }
}