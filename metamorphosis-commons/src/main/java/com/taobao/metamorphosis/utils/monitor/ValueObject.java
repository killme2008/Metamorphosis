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
package com.taobao.metamorphosis.utils.monitor;

import java.util.concurrent.atomic.AtomicReference;


/**
 * @author mengting
 * @date 2008-6-17
 */

public class ValueObject {
    public static final int NUM_VALUES = 2;

    private final AtomicReference<long[]> values = new AtomicReference<long[]>();


    public ValueObject() {
        final long[] init = new long[NUM_VALUES];
        this.values.set(init);
    }


    public ValueObject(final long value1, final long value2) {
        this();
        this.addCount(value1, value2);
    }


    public void addCount(final long value1, final long value2) {
        long[] current;
        final long[] update = new long[NUM_VALUES];
        do {
            current = values.get();
            update[0] = current[0] + value1;
            update[1] = current[1] + value2;
        } while (!values.compareAndSet(current, update));

    }


    /**
     * Should only be used by log writer to deduct written counts. This method
     * does not affect stat rules.
     */
    void deductCount(final long value1, final long value2) {
        long[] current;
        final long[] update = new long[NUM_VALUES];
        do {
            current = values.get();
            update[0] = current[0] - value1;
            update[1] = current[1] - value2;
        } while (!values.compareAndSet(current, update));
    }


    /**
     * 原子化读取所有值
     */
    public long[] getValues() {
        return values.get();
    }
}