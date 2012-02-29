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
package com.taobao.metamorphosis.tools.monitor;

/**
 * 代表监控系统启动初始化间段出现的异常
 * @author 无花
 * @since 2011-5-24 下午05:10:42
 */

public class InitException extends Exception {

    private static final long serialVersionUID = 5811163916323040678L;

    public InitException() {
        super();

    }

    public InitException(String message, Throwable cause) {
        super(message, cause);

    }

    public InitException(String s) {
        super(s);

    }

    public InitException(Throwable cause) {
        super(cause);

    }

}