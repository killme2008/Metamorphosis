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
package com.taobao.metamorphosis.server.assembly;

/**
 * 事务处理器MBean接口，提供一些查询和管理的API
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-29
 * 
 */
public interface TransactionalCommandProcessorMBean {

    /**
     * 返回所有处于prepare状态的xa事务
     * 
     * @return
     */
    public String[] getPreparedTransactions() throws Exception;


    /**
     * 返回所有处于prepare状态的xa事务数目
     * 
     * @return
     */
    public int getPreparedTransactionCount() throws Exception;


    /**
     * 人工提交事务
     * 
     * @param txKey
     */
    public void commitTransactionHeuristically(String txKey, boolean onePhase) throws Exception;


    /**
     * 人工回滚事务
     * 
     * @param txKey
     */
    public void rollbackTransactionHeuristically(String txKey) throws Exception;


    /**
     * 人工完成事务，不提交也不回滚，简单删除
     * 
     * @param txKey
     * @throws Exception
     */
    public void completeTransactionHeuristically(String txKey) throws Exception;

}