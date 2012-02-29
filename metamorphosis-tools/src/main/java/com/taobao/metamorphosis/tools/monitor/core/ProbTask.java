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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 探测线程专用task,确保不会抛出异常,以免schedule停止后续探测
 * 
 * @author 无花
 * @since 2011-5-30 下午01:56:49
 */

public abstract class ProbTask implements Runnable {
    protected Log log = LogFactory.getLog(this.getClass());


    public void run() {
        try {
            this.doExecute();
        }
        catch (InterruptedException e) {
            this.log.warn("探测线程接收到中断信号.");
            Thread.currentThread().interrupt();
        }
        catch (Throwable e) {
            // 捕获掉所有异常,以免ScheduledExecutorService不能执行后续探测任务
            this.handleExceptionInner(e);
        }
    }


    private void handleExceptionInner(Throwable e) {
        try {
            this.handleException(e);
        }
        catch (Throwable e2) {
            // ignore
        }
    }


    abstract protected void doExecute() throws Exception;


    abstract protected void handleException(Throwable e);

}