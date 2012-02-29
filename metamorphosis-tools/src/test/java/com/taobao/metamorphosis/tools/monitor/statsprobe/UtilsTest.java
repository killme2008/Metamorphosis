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

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.utils.Utils;
import com.taobao.metamorphosis.utils.Utils.Action;


/**
 * @author Œﬁª®
 * @since 2011-5-27 œ¬ŒÁ06:20:05
 */

public class UtilsTest {

    @Test
    public void testProcessEachLine() throws IOException {
        String realtimeStatsInfo =
                "result 200 150 2147483647\r\nSTATS\r\nrealtime_put null\r\nrealtime_get null\r\nrealtime_offset null\r\nrealtime_get_miss null\r\nrealtime_put_failed null\r\nrealtime_message_size null\r\nEND";

        com.taobao.metamorphosis.utils.Utils.processEachLine(realtimeStatsInfo,
            new com.taobao.metamorphosis.utils.Utils.Action() {
                boolean finished = false;


                public boolean isFinished() {
                    return finished;
                }


                @Override
                public void process(String line) {
                    if (line.startsWith("realtime_put_failed")) {
                        Assert.assertFalse(RealTimeStatsProber.isNeedAlert(line, 10));
                        finished = true;
                    }
                }
            });
    }


    @Test
    public void testProcessEachLine2() throws IOException {
        String realtimeStatsInfo =
                "result 200 150 2147483647\r\nSTATS\r\nrealtime_put null\r\nrealtime_get null\r\nrealtime_offset null\r\nrealtime_get_miss null\r\nrealtime_put_failed Count=1,Value=10,Value/Count=11,Count/Duration=,Duration=\r\nrealtime_message_size null\r\nEND";

        Utils.processEachLine(realtimeStatsInfo, new Action() {
            boolean finished = false;


            public boolean isFinished() {
                return finished;
            }


            @Override
            public void process(String line) {
                if (line.startsWith("realtime_put_failed")) {
                    Assert.assertTrue(RealTimeStatsProber.isNeedAlert(line, 10));
                    finished = true;
                }
            }
        });
    }


    @Test
    public void testProcessEachLine3() throws IOException {
        String realtimeStatsInfo =
                "result 200 150 2147483647\nSTATS\r\nrealtime_put null\nrealtime_get null\nrealtime_offset null\nrealtime_get_miss null\nrealtime_put_failed Count=1,Value=,Value/Count=11,Count/Duration=,Duration=\nrealtime_message_size null\nEND";

        Utils.processEachLine(realtimeStatsInfo, new Action() {
            boolean finished = false;


            public boolean isFinished() {
                return finished;
            }


            @Override
            public void process(String line) {
                if (line.startsWith("realtime_put_failed")) {
                    Assert.assertFalse(RealTimeStatsProber.isNeedAlert(line, 10));
                    finished = true;
                }
            }
        });
    }


    @Test
    public void testGetResourceAsProperties() throws IOException {
        Properties properties = Utils.getResourceAsProperties("utilstest.props", "GBK");
        Assert.assertTrue(properties.get("ss").equals("sss"));
        Assert.assertTrue(properties.get("bb").equals("≤‚ ‘"));
    }
}