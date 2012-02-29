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
 * 
 * 并发测试任务接口
 * 
 * @author boyan
 * 
 * @since 1.0, 2010-1-11 下午03:11:58
 */

public interface ConcurrentTestTask {
    /**
     * 
     * @param index
     *            线程索引号
     * @param times
     *            次数
     * @throws Exception TODO
     */
    public void run(int index, int times) throws Exception;
}