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

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber.ProbContext;

/**
 * @author 无花
 * @since 2011-5-25 上午11:53:10
 */

abstract public class ProbeListener {
    private static Logger listenerLogger = Logger.getLogger("probeListener");

    /**发送消息失败时**/
    abstract protected void onSendFail(MsgSender sender, SendResultWrapper result);

    /**接收消息失败时**/
    abstract protected void onReceiveFail(ProbContext probContext);

    protected Logger getLogger() {
        return listenerLogger;
    }

}