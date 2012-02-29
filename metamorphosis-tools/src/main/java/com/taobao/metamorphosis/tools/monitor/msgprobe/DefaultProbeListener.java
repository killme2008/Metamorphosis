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

import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ReveiceResult;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber.ProbContext;

/**
 * @author ÎÞ»¨
 * @since 2011-5-25 ÉÏÎç11:20:50
 */

public class DefaultProbeListener extends ProbeListener {
    
    @Override
    protected void onReceiveFail(ProbContext probContext) {
        ReveiceResult revResult = probContext.getReveiceResult();
        getLogger()
                .warn(
                        String
                                .format(
                                        "fail on receive message from[%s].\n %s second passed since last send msg;\n topic[%s];\n partition[%s];\n offset[%s]",
                                        revResult.getServerUrl(), probContext.getSendRevInterval(), revResult
                                                .getTopic(), revResult.getPartition(), revResult.getOffset()),
                        probContext.getReveiceResult().getException());
    }

    @Override
    protected void onSendFail(MsgSender sender, SendResultWrapper result) {
        getLogger().warn(
                String.format("fail send message to %s ;error[%s];", sender.getServerUrl(), result.getErrorMessage()),
                result.getException());
    }

}