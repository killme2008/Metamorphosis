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
package com.taobao.metamorphosis.tools.utils;

import java.util.Date;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç1:46:26
 */

public class MonitorResult {
    public String getKey() {
        return this.key;
    }


    public void setKey(String key) {
        this.key = key;
    }


    public String getDescribe() {
        return this.describe;
    }


    public void setDescribe(String describe) {
        this.describe = describe;
    }


    public Double getValue() {
        return this.value;
    }


    public void setValue(Double value) {
        this.value = value;
    }


    public Date getTime() {
        return this.time;
    }


    public void setTime(Date time) {
        this.time = time;
    }


    public String getIp() {
        return this.ip;
    }


    public void setIp(String ip) {
        this.ip = ip;
    }


    public int getId() {
        return this.id;
    }


    public void setId(int id) {
        this.id = id;
    }

    private int id;

    private String key;

    private String ip;

    private String describe;

    private Double value;

    private Date time;


    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}