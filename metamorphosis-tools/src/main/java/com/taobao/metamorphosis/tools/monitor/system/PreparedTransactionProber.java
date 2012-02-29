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

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.MonitorResult;
import com.taobao.metamorphosis.tools.utils.TransactionUtil;


/**
 * 监控事务挂起的数量
 * 
 * @author 无花
 * @since 2011-9-30 上午10:30:35
 */

public class PreparedTransactionProber extends SystemProber {

    public PreparedTransactionProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) throws Exception {
        int preparedTransactionCount =
                TransactionUtil.getPreparedTransactionCount(sender.getHost(), this.getMonitorConfig().getJmxPort());
        String msg = sender.getServerUrl() + " 事务挂起的数量达到" + preparedTransactionCount;
        this.logger.debug(msg);
        if (preparedTransactionCount >= this.getMonitorConfig().getPreparedTransactionCountThreshold()) {
            this.alert(msg);
        }
        return null;
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {

    }


    public static void main(String[] args) throws Exception {
        int preparedTransactionCount = TransactionUtil.getPreparedTransactionCount("10.232.102.184", 9999);
        System.out.println(preparedTransactionCount);
    }

}