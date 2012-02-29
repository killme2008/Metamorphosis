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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.monitor.alert.Alarm;
import com.taobao.metamorphosis.tools.monitor.core.AbstractProber;
import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ProbTask;
import com.taobao.metamorphosis.tools.monitor.core.StatsResult;
import com.taobao.metamorphosis.utils.Utils;
import com.taobao.metamorphosis.utils.Utils.Action;


/**
 * @author 无花
 * @since 2011-5-27 下午03:18:53
 */

public class RealTimeStatsProber extends AbstractProber {

    protected static Logger logger = Logger.getLogger(RealTimeStatsProber.class);


    public RealTimeStatsProber(CoreManager coreManager) {
        super(coreManager);
    }


    public void init() throws InitException {
    }


    @Override
    public void doProb() throws InterruptedException {
        for (final MsgSender sender : this.getSenders()) {
            this.futures.add(this.getProberExecutor().scheduleWithFixedDelay(new ProbTask() {

                @Override
                protected void doExecute() throws Exception {
                    if (logger.isDebugEnabled()) {
                        logger.debug("realtime stats prob...");
                    }
                    RealTimeStatsProber.this.probOnce(sender);
                }


                @Override
                protected void handleException(Throwable e) {
                    if (e instanceof InterruptedException) {
                        logger.warn("RealTimeStats prob thread Interrupted. broker server: " + sender.getServerUrl(), e);
                    }
                    else {
                        logger
                            .error(
                                "unexpected error in RealTimeStats prob thread. broker server: "
                                        + sender.getServerUrl(), e);
                    }
                }

            }, 0, this.getMonitorConfig().getStatsProbCycleTime(), TimeUnit.MILLISECONDS));
            logger.info("realtime stats prob started:" + sender.getServerUrl());
        }
    }

    private final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>();


    @Override
    protected void doStopProb() {
        cancelFutures(this.futures);
    }


    private void probOnce(MsgSender sender) throws InterruptedException {
        StatsResult statsResult = sender.getStats("realtime", this.getMonitorConfig().getSendTimeout());
        if (statsResult.isSuccess()) {
            try {
                this.processStatsInfo(statsResult);
            }
            catch (Exception e) {
                logger.warn("process realtime stats info error. string:" + statsResult.getStatsInfo(), e);
            }
        }
        else {
            logger.warn("get realtime stats fail.serverUrl:" + statsResult.getServerUrl(), statsResult.getException());
        }
    }


    private void processStatsInfo(final StatsResult statsResult) throws IOException {
        logger.debug(statsResult.getStatsInfo());
        if (StringUtils.isBlank(statsResult.getStatsInfo())) {
            logger.warn("realtime stats info is blank");
            return;
        }

        Utils.processEachLine(statsResult.getStatsInfo(), new Action() {
            boolean finished = false;


            @Override
            public boolean isBreak() {
                return this.finished;
            }


            @Override
            public void process(String line) {
                if (line.startsWith("realtime_put_failed")) {
                    RealTimeStatsProber.this.alertIfNeed(line, statsResult);
                    this.finished = true;
                }
            }
        });

    }


    void alertIfNeed(String line, StatsResult statsResult) {
        if (isNeedAlert(line, this.getMonitorConfig().getRealtimePutFailThreshold())) {
            String msg =
                    new StringBuilder().append("realtime_put_fail is great than")
                        .append(this.getMonitorConfig().getRealtimePutFailThreshold()).append(",").append(line)
                        .append(",serverUrl:").append(statsResult.getServerUrl()).toString();
            logger.warn(msg);
            Alarm.alert(msg, this.getMonitorConfig());
        }
    }


    static boolean isNeedAlert(String line, long threshold) {
        // 这里没作太多的字符串合法性判断,一般不会出现非约定的值。
        // 如果服务器返回了非约定的结果,在上层调用中统一捕获异常

        String realTimeValue = StringUtils.split(line, " ")[1];
        if (StringUtils.isBlank(realTimeValue) || realTimeValue.equalsIgnoreCase("null")) {
            if (logger.isDebugEnabled()) {
                logger.debug("realtime_put_failed value string maybe null:" + realTimeValue);
            }
            return false;
        }

        String realtimePutFailValueStr = StringUtils.split(realTimeValue, ",")[1];
        long realtimePutFailValue = 0;
        try {
            realtimePutFailValue = Integer.valueOf(StringUtils.splitByWholeSeparator(realtimePutFailValueStr, "=")[1]);
        }
        catch (Exception e) {
            return false;
        }
        if (realtimePutFailValue >= threshold) {
            return true;
        }
        return false;
    }

}