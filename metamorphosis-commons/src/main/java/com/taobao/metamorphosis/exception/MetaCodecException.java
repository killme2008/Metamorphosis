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
package com.taobao.metamorphosis.exception;

public class MetaCodecException extends RuntimeException {
    static final long serialVersionUID = -1L;


    public MetaCodecException() {
        super();

    }


    public MetaCodecException(String message, Throwable cause) {
        super(message, cause);

    }


    public MetaCodecException(String message) {
        super(message);

    }


    public MetaCodecException(Throwable cause) {
        super(cause);

    }

}