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
package com.taobao.metamorphosis.tools.monitor.alert;

import java.util.Arrays;
import java.util.List;

import com.taobao.metamorphosis.tools.monitor.core.MonitorConfig;


/**
 * @author 无花
 * @since 2011-5-25 下午04:07:28
 */

public class Alarm {

    public static boolean needAlert = true;

    static {
        // PushMsg.setWangwangTitle("metamorphosis-monitor-alert");
    }


    public static void alert(final String msg, final MonitorConfig monitorConfig) {

        if (needAlert) {
            start().wangwangs(monitorConfig.getWangwangList()).mobiles(monitorConfig.getMobileList()).alert(msg);
        }
    }


    public static EachAlarm start() {
        return new EachAlarm();
    }

    public static class EachAlarm {
        private List<String> wangwangList;
        private List<String> mobileList;
        private boolean isNeedMobileAlert = true;


        public EachAlarm wangwangs(final List<String> wangwangList) {
            this.wangwangList = wangwangList;
            return this;
        }


        public EachAlarm wangwangs(final String... wangwangs) {
            if (wangwangs != null && wangwangs.length > 0) {
                this.wangwangs(Arrays.asList(wangwangs));
            }
            return this;
        }


        public EachAlarm mobiles(final List<String> mobileList) {
            this.mobileList = mobileList;
            return this;
        }


        public EachAlarm mobileAlert(final boolean isNeedMobileAlert) {
            this.isNeedMobileAlert = isNeedMobileAlert;
            return this;
        }


        public void alert(final String msg) {

            if (needAlert) {
                if (this.wangwangList != null && !this.wangwangList.isEmpty()) {
                    for (final String wangwang : this.wangwangList) {
                        // PushMsg.alertByWW(wangwang, msg);
                        // TODO:报警
                    }
                }

                if (this.mobileList != null && !this.mobileList.isEmpty() && this.isNeedMobileAlert) {
                    for (final String mobile : this.mobileList) {
                        // PushMsg.alertByMobile(mobile, msg);
                        // TODO:报警
                    }
                }
            }
        }
    }

}