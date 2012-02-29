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
package com.taobao.metamorphosis.tools.monitor.msgprobe;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.monitor.alert.Alarm;
import com.taobao.metamorphosis.tools.monitor.core.MonitorConfig;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ReveiceResult;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber.ProbContext;


/**
 * @author 无花
 * @since 2011-5-25 上午11:25:59
 */
// note:发旺旺消息字数有限制
public class AlarmProbeListener extends ProbeListener {

    private final MonitorConfig monitorConfig;
    static final private String wwTitle = "metamorphosis monitor alert";


    public AlarmProbeListener(final MonitorConfig monitorConfig) {
        this.monitorConfig = monitorConfig;
        // PushMsg.setWangwangTitle(wwTitle);
    }


    @Override
    protected void onReceiveFail(final ProbContext probContext) {
        final ReveiceResult revResult = probContext.getReveiceResult();
        final String exception =
                revResult.getException() != null ? revResult.getException().getMessage() : StringUtils.EMPTY;
        final String msg =
                String
                    .format(
                        "fail on receive message from[%s].\n %s second passed since last send msg;\n topic[%s];\n partition[%s];\n offset[%s];\n exception[%s]",
                        revResult.getServerUrl(), probContext.getSendRevInterval(), revResult.getTopic(),
                        revResult.getPartition(), revResult.getOffset(), exception);
        this.getLogger().warn("alarm...");
        Alarm.alert(msg, monitorConfig);
    }


    @Override
    protected void onSendFail(final MsgSender sender, final SendResultWrapper result) {

        final String exception = result.getException() != null ? result.getException().getMessage() : StringUtils.EMPTY;
        final String msg =
                String.format("fail send message to %s ; errorMsg[%s];\n exception[%s]", sender.getServerUrl(),
                    result.getErrorMessage(), exception);

        this.getLogger().warn("alarm...");
        Alarm.alert(msg, monitorConfig);
    }

}