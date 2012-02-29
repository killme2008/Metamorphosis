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
package com.taobao.metamorphosis.client.consumer;

/**
 * ∂©‘ƒ’ﬂ–≈œ¢
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public class SubscriberInfo {
    private final MessageListener messageListener;
    private final int maxSize;


    public SubscriberInfo(final MessageListener messageListener, final int maxSize) {
        super();
        this.messageListener = messageListener;
        this.maxSize = maxSize;
    }


    public MessageListener getMessageListener() {
        return this.messageListener;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.maxSize;
        result = prime * result + (this.messageListener == null ? 0 : this.messageListener.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final SubscriberInfo other = (SubscriberInfo) obj;
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.messageListener == null) {
            if (other.messageListener != null) {
                return false;
            }
        }
        else if (!this.messageListener.equals(other.messageListener)) {
            return false;
        }
        return true;
    }


    public int getMaxSize() {
        return this.maxSize;
    }

}