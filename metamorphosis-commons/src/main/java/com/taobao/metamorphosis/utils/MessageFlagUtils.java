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
package com.taobao.metamorphosis.utils;

import com.taobao.metamorphosis.Message;


/**
 * 消息flag工具类 flag是32位整数，它的结构如下：</br></br>
 * <ul>
 * <li>低一位，1表示有消息属性，否则没有</li>
 * <li>其他暂时保留</li>
 * </ul>
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public class MessageFlagUtils {

    public static int getFlag(final Message message) {
        int flag = 0;
        if (message != null && message.getAttribute() != null) {
            // 低一位设置为1
            flag = flag & 0xFFFFFFFE | 1;
        }
        return flag;
    }


    public static boolean hasAttribute(final int flag) {
        return (flag & 0x1) == 1;
    }

}