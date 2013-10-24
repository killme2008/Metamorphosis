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
package com.taobao.metamorphosis;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 为了访问message中包级别的方法提供的辅助类
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
public class MessageAccessor {
    public static void setId(Message message, long id) {
        message.setId(id);
    }


    public static void setFlag(Message message, int flag) {
        message.setFlag(flag);
    }


    public static boolean isRollbackOnly(Message message) {
        return message.isRollbackOnly();
    }


    public static int getFlag(Message message) {
        return message.getFlag();
    }


    public static void setPartition(Message message, Partition partition) {
        message.setPartition(partition);
    }
}