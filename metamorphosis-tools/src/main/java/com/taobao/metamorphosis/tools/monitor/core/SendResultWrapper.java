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
package com.taobao.metamorphosis.tools.monitor.core;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.producer.SendResult;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-5-25 ÉÏÎç11:57:30
 */

public class SendResultWrapper {

    private Message message;

    private SendResult sendResult;

    private Exception e;


    public SendResultWrapper setMessage(Message message) {
        this.message = message;
        return this;
    }


    public Message getMessage() {
        return this.message;
    }


    public SendResult getSendResult() {
        return this.sendResult;
    }


    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }


    public boolean isSuccess() {
        return this.sendResult != null && this.sendResult.isSuccess();
    }


    public Exception getException() {
        return this.e;
    }


    public void setException(Exception e) {
        this.e = e;
    }


    public String getErrorMessage() {
        return this.sendResult != null ? this.sendResult.getErrorMessage() : "sendResult is null";
    }
}