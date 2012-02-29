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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.monitor.core.AbstractProber;
import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgReceiver;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ProbTask;
import com.taobao.metamorphosis.tools.monitor.core.ReveiceResult;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;


/**
 * @author 无花
 * @since 2011-5-24 下午01:52:50
 */

public class MsgProber extends AbstractProber {

    // private static Logger logger = Logger.getLogger(MsgProber.class);

    private final List<ProbeListener> probeListeners = new ArrayList<ProbeListener>();

    private InnerProbeListener innerProbeListener;

    private volatile AtomicBoolean isInited = new AtomicBoolean(false);


    public MsgProber(CoreManager coreManager) {
        super(coreManager);
    }


    public void init() throws InitException {
        if (this.isInited.compareAndSet(false, true)) {
            try {
                this.innerProbeListener = new InnerProbeListener();

                this.logger.info("publish topics...");
                this.publishTopics(this.getSenders());
                this.addListener(new DefaultProbeListener());
                this.addListener(new AlarmProbeListener(this.getMonitorConfig()));
            }
            catch (Exception e) {
                throw new InitException("unexpected errer at init", e);
            }
        }
    }

    private final class ProbOneBrokerTask extends ProbTask {
        MsgSender sender;
        MsgReceiver receiver;
        MsgProber prober;


        ProbOneBrokerTask(MsgSender sender, MsgReceiver receiver, MsgProber prober) {
            this.sender = sender;
            this.receiver = receiver;
            this.prober = prober;
        }


        @Override
        protected void doExecute() throws Exception {
            if (MsgProber.this.getLogger().isDebugEnabled()) {
                MsgProber.this.getLogger().debug("msg prob...");
            }
            this.prober.prob(this.sender, this.receiver);
        }


        @Override
        protected void handleException(Throwable e) {
            MsgProber.this.logger.error(
                "unexpected error in msg prob thread. broker server: " + this.sender.getServerUrl(), e);
        }

    }

    private final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>();


    @Override
    public void doProb() throws InterruptedException {
        // 保证一对sender和reveicer同一时间内只能在一个线程中使用
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.getSenders().length; i++) {
            this.futures.add(this.getProberExecutor().scheduleWithFixedDelay(
                new ProbOneBrokerTask(this.getSenders()[i], this.getReveicers()[i], this), 0,
                this.getMonitorConfig().getMsgProbeCycleTime(), TimeUnit.MILLISECONDS));
            sb.append("msg probe started:").append(this.getSenders()[i].getServerUrl()).append("\n");
        }
        this.logger.info(sb.toString());
    }


    @Override
    protected void doStopProb() {
        cancelFutures(this.futures);
    }


    public void addListener(ProbeListener listener) {
        this.probeListeners.add(listener);
    }


    private void prob(MsgSender sender, MsgReceiver receiver) throws InterruptedException {
        SendResultWrapper sendResult = sender.sendMessage(this.getMonitorConfig().getSendTimeout());
        ProbContext probContext = new ProbContext();
        probContext.lastSendTime = System.currentTimeMillis();
        probContext.sendResult = sendResult;

        if (!sendResult.isSuccess()) {
            this.innerProbeListener.onSendFail(sender, sendResult);
            return;
        }

        Thread.sleep(this.getMonitorConfig().getProbeInterval());

        // receive采用直连接收,.
        // 直连发送时返回的patition为brokerId=-1,patition=真实的patition
        ReveiceResult reveiceResult = receiver.get(sender.getTopic(), sendResult.getSendResult().getPartition());
        probContext.lastRevTime = System.currentTimeMillis();
        probContext.reveiceResult = reveiceResult;
        if (!reveiceResult.isSuccess()) {
            this.innerProbeListener.onReceiveFail(probContext);
        }

        // 下一次接收消息的offset从这一次发送的offset位置开始
        receiver.setOffset(sendResult.getSendResult().getPartition(), sendResult.getSendResult().getOffset());
    }


    private void publishTopics(MsgSender[] senders) {
        for (int i = 0; i < senders.length; i++) {
            senders[i].publish();
        }
    }

    final private class InnerProbeListener extends ProbeListener {

        @Override
        public void onSendFail(MsgSender sender, SendResultWrapper result) {
            for (ProbeListener probeListener : MsgProber.this.probeListeners) {
                try {
                    probeListener.onSendFail(sender, result);
                }
                catch (Exception e) {
                    MsgProber.this.logger.error("处理发送失败事件时发生错误," + probeListener.getClass(), e);
                }
            }
        }


        @Override
        public void onReceiveFail(ProbContext probContext) {
            for (ProbeListener probeListener : MsgProber.this.probeListeners) {
                try {
                    probeListener.onReceiveFail(probContext);
                }
                catch (Exception e) {
                    MsgProber.this.logger.error("处理接收失败事件时发生错误," + probeListener.getClass(), e);
                }
            }
        }

    }

    static public class ProbContext {
        SendResultWrapper sendResult;
        ReveiceResult reveiceResult;
        long lastSendTime;// 上一次发送消息的时间,毫秒
        long lastRevTime; // 本次接收到消息的时间,毫秒


        public long getSendRevInterval() {
            return (this.lastRevTime - this.lastSendTime) / 1000;
        }


        public SendResultWrapper getSendResult() {
            return this.sendResult;
        }


        public void setSendResult(SendResultWrapper sendResult) {
            this.sendResult = sendResult;
        }


        public ReveiceResult getReveiceResult() {
            return this.reveiceResult;
        }


        public void setReveiceResult(ReveiceResult reveiceResult) {
            this.reveiceResult = reveiceResult;
        }


        public long getLastSendTime() {
            return this.lastSendTime;
        }


        public void setLastSendTime(long lastSendTime) {
            this.lastSendTime = lastSendTime;
        }


        public long getLastRevTime() {
            return this.lastRevTime;
        }


        public void setLastRevTime(long lastRevTime) {
            this.lastRevTime = lastRevTime;
        }
    }

}