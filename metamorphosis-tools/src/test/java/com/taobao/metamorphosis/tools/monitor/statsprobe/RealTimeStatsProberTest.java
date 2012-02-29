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
package com.taobao.metamorphosis.tools.monitor.statsprobe;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.tools.monitor.statsprobe.RealTimeStatsProber;

/**
 *
 * @author ÎÞ»¨
 * @since 2011-5-27 ÏÂÎç05:48:39
 */

public class RealTimeStatsProberTest {
    
    @Test
    public void testIsNeedAlert() {
        Assert.assertFalse(RealTimeStatsProber.isNeedAlert("realtime_put_failed null", 10));
        Assert.assertTrue(RealTimeStatsProber.isNeedAlert("realtime_put_failed Count=1,Value=10,Value/Count=11,Count/Duration=,Duration=", 10));
        Assert.assertFalse(RealTimeStatsProber.isNeedAlert("realtime_put_failed Count=1,Value=9,Value/Count=11,Count/Duration=,Duration=", 10));
    }

}