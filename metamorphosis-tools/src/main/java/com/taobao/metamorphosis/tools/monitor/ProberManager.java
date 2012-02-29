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
package com.taobao.metamorphosis.tools.monitor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MonitorConfig;
import com.taobao.metamorphosis.tools.monitor.core.Prober;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber;
import com.taobao.metamorphosis.tools.monitor.offsetcompareprob.OffsetCompareProber;
import com.taobao.metamorphosis.tools.monitor.offsetprob.OffsetProber;
import com.taobao.metamorphosis.tools.monitor.statsprobe.RealTimeStatsProber;
import com.taobao.metamorphosis.tools.monitor.system.CPULoadProber;
import com.taobao.metamorphosis.tools.monitor.system.DiskUsedProber;
import com.taobao.metamorphosis.tools.monitor.system.JvmMemoryProber;
import com.taobao.metamorphosis.tools.monitor.system.MetaConnProber;
import com.taobao.metamorphosis.tools.monitor.system.NetWorkUsedProber;
import com.taobao.metamorphosis.tools.monitor.system.PreparedTransactionProber;
import com.taobao.metamorphosis.tools.monitor.system.ProcessResultHook;
import com.taobao.metamorphosis.tools.monitor.system.SystemProber;
import com.taobao.metamorphosis.tools.monitor.system.ZKConnProber;


/**
 * @author 无花
 * @since 2011-5-27 下午03:28:14
 */
// TODO 先简单这样处理.以后可以进一步分离各种prober与调度逻辑
public class ProberManager {

    private static Logger logger = Logger.getLogger(ProberManager.class);

    private final Map<Class<? extends Prober>, Prober> probers = new HashMap<Class<? extends Prober>, Prober>();
    private MonitorConfig monitorConfig;
    private final AtomicBoolean isInited = new AtomicBoolean(false);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);


    void register(Prober prober) {
        this.probers.put(prober.getClass(), prober);
    }


    public void initProbers(CoreManager coreManager) throws InitException {
        if (this.isInited.compareAndSet(false, true)) {
            try {
                this.monitorConfig = coreManager.getMonitorConfig();
                this.register(new MsgProber(coreManager));
                this.register(new RealTimeStatsProber(coreManager));
//                this.register(new OffsetProber(coreManager));
                this.register(new OffsetCompareProber(coreManager));
                this.register(new CPULoadProber(coreManager));
                this.register(new DiskUsedProber(coreManager));
                this.register(new NetWorkUsedProber(coreManager));
                this.register(new JvmMemoryProber(coreManager));
                this.register(new ZKConnProber(coreManager));
                this.register(new MetaConnProber(coreManager));
                this.register(new PreparedTransactionProber(coreManager));

                for (Prober prober : this.probers.values()) {
                    prober.init();
                    logger.info(prober.getClass().getSimpleName() + " init success!");
                }

                logger.info("all probers init success!");

            }
            catch (Throwable e) {
                throw new InitException(e);
            }
        }
    }


    public void setHookToAllSystemMonitor(ProcessResultHook hook) {
        if (!this.isInited.get()) {
            throw new IllegalStateException("not inited");
        }

        for (Prober prober : this.probers.values()) {
            if (prober instanceof SystemProber) {
                ((SystemProber) prober).setProcessResultHook(hook);
            }
        }
    }


    public void initProbers(String source) throws InitException {
        this.monitorConfig = new MonitorConfig();
        try {
            this.monitorConfig.loadInis(StringUtils.isBlank(source) ? MonitorConfig.DEFAULT_CONFIG_FILE : source);
        }
        catch (IOException e) {
            throw new InitException(e);
        }
        this.initProbers(CoreManager.getInstance(this.monitorConfig, this.monitorConfig.getMetaServerList().size()
                * this.probers.size()));
    }


    public void startProb() throws Exception {
        if (this.isInited.get()) {
            for (Prober prober : this.probers.values()) {
                prober.prob();
            }
            this.isStarted.set(true);
        }
        else {
            throw new InitException("has not inited yet");
        }
    }


    public void stopProb() throws Exception {
        if (this.isInited.get()) {
            for (Prober prober : this.probers.values()) {
                prober.stopProb();
            }
            this.isStarted.set(false);
        }
        else {
            throw new InitException("has not inited yet");
        }
    }


    MonitorConfig getMonitorConfig() {
        return this.monitorConfig;
    }


    public static Logger getLogger() {
        return logger;
    }


    public boolean isStarted() {
        return this.isStarted.get();
    }

}