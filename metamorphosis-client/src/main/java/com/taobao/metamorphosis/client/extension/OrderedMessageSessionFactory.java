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
package com.taobao.metamorphosis.client.extension;

import com.taobao.metamorphosis.client.MessageSessionFactory;


/**
 * <pre>
 * 消息会话工厂，meta客户端的主接口,推荐一个应用只使用一个.
 * 需要按照消息内容(例如某个id)散列到固定分区并要求有序的场景中使用.
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-24 下午4:30:36
 */

public interface OrderedMessageSessionFactory extends MessageSessionFactory {

}