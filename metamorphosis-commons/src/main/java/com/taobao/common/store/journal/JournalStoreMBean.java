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
 *   dogun (yuexuqiang at gmail.com)
 */
package com.taobao.common.store.journal;

import java.io.IOException;


/**
 * 日志方式存储的MBean
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
public interface JournalStoreMBean {

    /**
     * 获得所有有效数据文件的信息
     * 
     * @return 所有有效数据文件的信息
     */
    String getDataFilesInfo();


    /**
     * 获得所有有效日志文件的信息
     * 
     * @return 所有有效日志文件的信息
     */
    String getLogFilesInfo();


    /**
     * 获取当前的文件编号
     * 
     * @return 当前的文件编号
     */
    int getNumber();


    /**
     * 获取存储的路径
     * 
     * @return the path
     */
    String getPath();


    /**
     * 获取存储的名字
     * 
     * @return the name
     */
    String getName();


    /**
     * 获取当前数据文件信息
     * 
     * @return 当前数据文件信息
     */
    String getDataFileInfo();


    /**
     * 获取当前日志文件信息
     * 
     * @return 当前日志文件信息
     */
    String getLogFileInfo();


    /**
     * 查看索引的信息。<b>注意:</b>该操作可能会撑暴内存
     * 
     * @return 所有的索引信息
     */
    String viewIndexMap();


    /**
     * 获得数据的个数
     * 
     * @return 数据的个数
     * @throws IOException
     */
    long getSize() throws IOException;


    /**
     * 对数据文件进行检查，并作出相应的处理
     * 
     * @throws IOException
     */
    void check() throws IOException;


    long getIntervalForRemove();


    void setIntervalForRemove(long interval);


    long getIntervalForCompact();


    void setIntervalForCompact(long interval);
}