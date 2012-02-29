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
 *   wuhua <wq163@163.com> 
 */
package com.taobao.metamorphosis.metaslave;

/**
 * 代表一个启动订阅master消息时的错误
 * 
 * @author 无花
 * @since 2011-6-28 下午03:35:30
 */

public class SubscribeMasterMessageException extends RuntimeException {

    private static final long serialVersionUID = 3449735809236405427L;


    public SubscribeMasterMessageException() {
        super();

    }


    public SubscribeMasterMessageException(final String message, final Throwable cause) {
        super(message, cause);

    }


    public SubscribeMasterMessageException(final String message) {
        super(message);

    }


    public SubscribeMasterMessageException(final Throwable cause) {
        super(cause);

    }
}