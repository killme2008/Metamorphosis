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

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 表示某topic当前可用的分区个数不正确,比如跟期望的总数不一致等
 * 
 * @author 无花
 * @since 2011-8-2 下午02:49:27
 */

public class AvailablePartitionNumException extends MetaClientException {

    private static final long serialVersionUID = 8087499474643513774L;


    public AvailablePartitionNumException() {
        super();
    }


    public AvailablePartitionNumException(String message, Throwable cause) {
        super(message, cause);
    }


    public AvailablePartitionNumException(String message) {
        super(message);
    }


    public AvailablePartitionNumException(Throwable cause) {
        super(cause);
    }

}