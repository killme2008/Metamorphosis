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
package com.taobao.metamorphosis.tools.monitor.core;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.tools.domain.MetaServer;
import com.taobao.metamorphosis.tools.monitor.InitException;


/**
 * @author 无花
 * @since 2011-5-27 下午03:28:14
 */

public class CoreManager {

    private static Logger logger = Logger.getLogger(CoreManager.class);

    private static CoreManager instance;

    private MsgSender[] senders;
    private MsgReceiver[] reveicers;
    private volatile MonitorConfig monitorConfig;
    private final ScheduledThreadPoolExecutor proberExecutor;


    private CoreManager(final MonitorConfig monitorConfig, int coreSize) throws InitException {
        this.monitorConfig = monitorConfig;
        List<MetaServer> metaServerList = this.monitorConfig.getMetaServerList();
        if (metaServerList == null || metaServerList.isEmpty()) {
            throw new InitException("serverUrls不能为空");
        }

        this.proberExecutor = new ScheduledThreadPoolExecutor(coreSize);

        logger.info("init senders and receivers...");
        this.initSenderReceiver();
        monitorConfig.addPropertyChangeListener("serverUrlList", new PropertyChangeListener() {

            public void propertyChange(PropertyChangeEvent evt) {
                logger.info("服务器列表发生变化");
                CoreManager.this.proberExecutor.setCorePoolSize(monitorConfig.getMetaServerList().size() * 2);
                try {
                    CoreManager.this.reInitSenderReceiver();
                    logger.info("重新初始化监控对象成功");
                }
                catch (Exception e) {
                    logger.error("重新初始化监控对象失败!", e);
                }

            }
        });
    }


    synchronized private void reInitSenderReceiver() throws InitException {
        if (this.senders != null) {
            for (MsgSender sender : this.senders) {
                if (sender != null) {
                    sender.dispose();
                    sender = null;
                }
            }
            this.senders = null;
        }
        if (this.reveicers != null) {
            for (MsgReceiver reveicer : this.reveicers) {
                if (reveicer != null) {
                    reveicer.dispose();
                    reveicer = null;
                }
            }
            this.reveicers = null;
        }
        this.initSenderReceiver();
    }


    synchronized private void initSenderReceiver() throws InitException {

        List<MetaServer> metaServerList = this.monitorConfig.getMetaServerList();
        Collections.sort(metaServerList);
        this.senders = new MsgSender[metaServerList.size()];
        this.reveicers = new MsgReceiver[metaServerList.size()];
        try {
            for (int i = 0; i < metaServerList.size(); i++) {
                logger.info("init for:" + metaServerList.get(i));
                this.senders[i] = new MsgSender(metaServerList.get(i).getUrl(), "0", this.monitorConfig);

                this.reveicers[i] = new MsgReceiver(metaServerList.get(i).getUrl(), this.monitorConfig);
            }
        }
        catch (MetaClientException e) {
            throw new InitException("init Senders and Receivers fail", e);
        }
    }


    synchronized public static CoreManager getInstance(MonitorConfig monitorConfig, int coreSize) throws InitException {
        /** 这里不会频繁调用多次,简化处理,整个方法同步掉 **/
        return instance == null ? (instance = new CoreManager(monitorConfig, coreSize)) : instance;
    }


    public MsgSender[] getSenders() {
        return this.senders;
    }


    public MsgSender getSender(String serverUrl) {
        for (MsgSender sender : this.senders) {
            if (sender.getServerUrl().contains(serverUrl)) {
                return sender;
            }
        }
        return null;
    }


    MsgReceiver[] getReveicers() {
        return this.reveicers;
    }


    public MonitorConfig getMonitorConfig() {
        return this.monitorConfig;
    }


    public ScheduledThreadPoolExecutor getProberExecutor() {
        return this.proberExecutor;
    }

}