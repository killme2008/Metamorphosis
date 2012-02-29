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

import java.util.concurrent.CyclicBarrier;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2010-1-11 ÏÂÎç03:12:41
 */

public class ConcurrentTestRunner implements Runnable {
    private CyclicBarrier barrier;

    private ConcurrentTestTask task;

    private int repeatCount;

    private int index;


    public ConcurrentTestRunner(CyclicBarrier barrier, ConcurrentTestTask task, int repeatCount, int index) {
        super();
        this.barrier = barrier;
        this.task = task;
        this.repeatCount = repeatCount;
        this.index = index;
    }


    public void run() {
        try {
            barrier.await();
            for (int i = 0; i < repeatCount; i++) {
                task.run(this.index, i);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                barrier.await();
            }
            catch (Exception e) {
                // ignore
            }
        }
    }
}