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
package com.taobao.metamorphosis.client.extension.producer;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * 滑动窗口,用来控制流量
 * 
 * @author 无花
 * @since 2011-10-26 上午10:48:46
 */

class SlidingWindow {

    private final Semaphore semaphore;
    private final ItemUnit itemUnit;
    private final int itemUnitSize;


    SlidingWindow(int itemUnitSize) {
        this(itemUnitSize, new DefaultItemUnit());
    }


    SlidingWindow(int itemUnitSize, ItemUnit itemUnit) {
        this.itemUnitSize = itemUnitSize;
        this.semaphore = new Semaphore(this.itemUnitSize);
        this.itemUnit = itemUnit;
    }


    public boolean tryAcquireByLength(int lenth) {
        return this.semaphore.tryAcquire(this.covertToItemLength(lenth));
    }


    public boolean tryAcquireByLength(int length, long timeout, TimeUnit unit) throws InterruptedException {
        return this.semaphore.tryAcquire(this.covertToItemLength(length), timeout, unit);
    }


    public void releaseByLenth(int length) {
        this.semaphore.release(this.covertToItemLength(length));
    }


    public int getWindowsSize() {
        return itemUnitSize;
    }


    private int covertToItemLength(int length) {
        return this.itemUnit.covertToItemLength(length);
    }

    interface ItemUnit {
        int covertToItemLength(int lenth);
    }

    static class DefaultItemUnit implements ItemUnit {

        @Override
        public int covertToItemLength(int length) {
            // 4k流量占窗口的一个单位
            int i = length / 4096;
            return i > 0 ? i : 1;
        }
    }

}