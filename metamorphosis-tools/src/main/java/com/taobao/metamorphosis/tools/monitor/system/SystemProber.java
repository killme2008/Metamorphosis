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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.monitor.alert.Alarm;
import com.taobao.metamorphosis.tools.monitor.core.AbstractProber;
import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ProbTask;
import com.taobao.metamorphosis.tools.utils.MonitorResult;


/**
 * 对于cpu,内存,磁盘等系统监控的共同点抽象
 * 
 * @author 无花
 * @since 2011-9-28 下午2:39:43
 */

public abstract class SystemProber extends AbstractProber {

    protected ProcessResultHook processResultHook = new ProcessResultHook() {
    };

    private final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>();


    public SystemProber(CoreManager coreManager) {
        super(coreManager);
    }


    public void init() throws InitException {

    }


    @Override
    protected void doProb() throws InterruptedException {
        for (final MsgSender sender : this.getSenders()) {
            this.futures.add(this.getProberExecutor().scheduleAtFixedRate(new ProbTask() {

                @Override
                protected void handleException(Throwable e) {
                    SystemProber.this.logger.error("unexpected error in offset prob thread.", e);
                }


                @Override
                protected void doExecute() throws Exception {
                    MonitorResult monitorResult = SystemProber.this.getMonitorResult(sender);
                    SystemProber.this.logger.debug(SystemProber.this.getClass().getSimpleName() + " prob result:"
                            + monitorResult);
                    SystemProber.this.processResult0(monitorResult);
                }

            }, 1, this.getMonitorConfig().getSystemProbCycleTime(), TimeUnit.MINUTES));
        }
        this.logger.info(this.getClass().getSimpleName() + " prober started");
    }


    protected void processResult0(MonitorResult monitorResult) throws Exception {
        if (monitorResult != null) {
            this.processResult(monitorResult);
            this.processResultHook.handler(monitorResult);
        }
    }


    @Override
    protected void doStopProb() {
        cancelFutures(this.futures);
        this.logger.info(this.getClass().getSimpleName() + " prober stoped");
    }


    /** 获取一次监控结果 */
    protected abstract MonitorResult getMonitorResult(MsgSender sender) throws Exception;


    /** 一次监控结果的处理 */
    protected abstract void processResult(MonitorResult monitorResult);


    public void setProcessResultHook(ProcessResultHook processResultHook) {
        this.processResultHook = processResultHook;
    }


    protected void alert(String msg) {
        this.logger.warn(msg);
        Alarm.alert(msg, this.getMonitorConfig());

    }

}