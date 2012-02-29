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
package com.taobao.metamorphosis.utils.test;

/**
 * 
 * 用于测试的时间监视器
 * 
 * @author boyan
 * 
 * @since 1.0, 2010-1-11 下午03:09:20
 */

public final class ClockWatch implements Runnable {
    private long startTime;
    private long stopTime;


    @Override
    public synchronized void run() {
        if (this.startTime == -1) {
            this.startTime = System.nanoTime();
        }
        else {
            this.stopTime = System.nanoTime();
        }

    }


    public synchronized void start() {
        this.startTime = -1;
    }


    public synchronized long getDurationInNano() {
        return this.stopTime - this.startTime;
    }


    public synchronized long getDurationInMillis() {
        return (this.stopTime - this.startTime) / 1000000;
    }
}