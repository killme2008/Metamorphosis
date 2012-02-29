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

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.query.Query;
import com.taobao.metamorphosis.tools.utils.ConnectionUtil;
import com.taobao.metamorphosis.tools.utils.MonitorResult;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * 监控meta是否正常连接到zk
 * 
 * @author 无花
 * @since 2011-9-29 下午1:53:47
 */

public class ZKConnProber extends SystemProber {
    ZKConfig zkConfig;


    public ZKConnProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    public void init() throws InitException {
        try {
            Query query = new Query();
            this.zkConfig = query.getZkConfig(this.coreManager.getMonitorConfig().getConfigPath());
            query.close();
        }
        catch (IOException e) {
            throw new InitException("初始化ZKConnProber失败", e);
        }
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) throws Exception {
        String[] zks = StringUtils.split(this.zkConfig.zkConnect, ",");
        boolean isConnected = false;
        for (int i = 0; i < 2; i++) {
            for (String zk : zks) {
                isConnected =
                        ConnectionUtil.IsConnected(sender.getHost(), zk.trim(), this.getMonitorConfig().getLoginUser(),
                            this.getMonitorConfig().getLoginPassword());
                if (isConnected) {
                    this.logger.debug(sender.getServerUrl() + "已连接到zk:" + zk);
                    break;
                }
            }

            if (isConnected) {
                break;
            }

            if (i < 1) {
            	logger.debug("Meta server[" + sender.getHost() + "]与zk服务器[" + this.zkConfig.zkConnect + "]第一次连接失败.");
                Thread.sleep(2000);
            }
        }

        if (!isConnected) {
            this.alert("Meta server[" + sender.getHost() + "]与zk服务器[" + this.zkConfig.zkConnect + "]之间没有网络连接.");
        }

        return null;
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        // do nothing

    }

}