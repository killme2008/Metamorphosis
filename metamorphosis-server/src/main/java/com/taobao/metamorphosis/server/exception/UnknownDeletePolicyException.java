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
package com.taobao.metamorphosis.server.exception;

public class UnknownDeletePolicyException extends RuntimeException {

    private static final long serialVersionUID = 4426831758552509034L;


    public UnknownDeletePolicyException() {
        super();

    }


    public UnknownDeletePolicyException(String message, Throwable cause) {
        super(message, cause);

    }


    public UnknownDeletePolicyException(String message) {
        super(message);

    }


    public UnknownDeletePolicyException(Throwable cause) {
        super(cause);

    }

}