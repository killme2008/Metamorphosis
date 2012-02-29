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
package com.taobao.metamorphosis.tools.monitor.system;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.ConnectionUtil;
import com.taobao.metamorphosis.tools.utils.MonitorResult;
import com.taobao.metamorphosis.utils.Utils;
import com.taobao.metamorphosis.utils.Utils.Action;


/**
 * 监控连接到Meta服务器的网络连接情况
 * 
 * @author 无花
 * @since 2011-9-29 下午1:53:47
 */

public class MetaConnProber extends SystemProber {

    public MetaConnProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(final MsgSender sender) throws Exception {
        String result =
                ConnectionUtil.getConnectionInfo(sender.getHost(), sender.getPort(), this.getMonitorConfig()
                    .getLoginUser(), this.getMonitorConfig().getLoginPassword());
        if (StringUtils.isBlank(result)) {
            // 没有连接,报警?
            this.logger.warn("没有客户端连接,meta: " + sender.getServerUrl());
            return null;
        }

        final AtomicInteger connSum = new AtomicInteger(0);
        Utils.processEachLine(result, new Action() {

            @Override
            public void process(String line) {
                String[] tmp = StringUtils.split(line, " ");
                if (tmp != null && tmp.length == 2) {
                    int count = Integer.parseInt(tmp[0]);
                    connSum.addAndGet(count);
                    if (count >= MetaConnProber.this.getMonitorConfig().getMetaConnectionPerIpThreshold()) {
                        MetaConnProber.this.alert("客户端[" + tmp[1] + "]与Meta服务器" + sender.getServerUrl() + "的网络连接数到达["
                                + count + "]个.");
                    }
                }
            }
        });

        String msg = "客户端连接到Meta服务器[" + sender.getServerUrl() + "]的网络连接总数到达[" + connSum.get() + "]个.";
        this.logger.debug(msg);
        if (connSum.get() >= MetaConnProber.this.getMonitorConfig().getMetaConnectionThreshold()) {
            this.alert(msg);
        }

        return null;
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        // do nothing
    }

}