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

import javax.management.ObjectInstance;
import javax.management.ObjectName;


/**
 * 
 * @author 无花
 * @since 2011-9-30 上午11:20:08
 */

public class TransactionUtil {
    final static ObjectName transactionObjectName;
    static {
        try {
            transactionObjectName =
                    new ObjectName("com.taobao.metamorphosis.server.assembly:type=TransactionalCommandProcessor,*");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 返回所有处于prepare状态的xa事务
     * 
     * @return
     */
    public static String[] getPreparedTransactions(String ip, int port) throws Exception {
    	JMXClient jmxClient = getJMXClient(ip, port);
    	String[] result = (String[])jmxClient.getAttribute(transactionObjectName, "PreparedTransactions");
    	jmxClient.close();
    	return result;
//        return (String[]) getJMXClient(ip, port).getAttribute(transactionObjectName, "PreparedTransactions");
    }


    /**
     * 返回所有处于prepare状态的xa事务数目
     * 
     * @return
     */
    public static int getPreparedTransactionCount(String ip, int port) throws Exception {
    	JMXClient jmxClient = getJMXClient(ip, port);
    	Integer result = (Integer)jmxClient.getAttribute(transactionObjectName, "PreparedTransactionCount");
    	jmxClient.close();
    	return result;
//        return (Integer) getJMXClient(ip, port).getAttribute(transactionObjectName, "PreparedTransactionCount");
    }


    /**
     * 提交事务
     * 
     * @param txKey
     */
    public static void commitTransaction(String txKey, boolean onePhase, String ip, int port) throws Exception {
        JMXClient jmxClient = getJMXClient(ip, port);
        ObjectInstance transactionObjectInstance = jmxClient.queryMBeanForOne(transactionObjectName);
        jmxClient.invoke(transactionObjectInstance.getObjectName(), "commitTransaction",
            new Object[] { txKey, onePhase }, new String[] { "java.lang.String", "boolean" });
        jmxClient.close();
    }


    /**
     * 回滚事务
     * 
     * @param txKey
     */
    public static void rollbackTransaction(String txKey, String ip, int port) throws Exception {
        JMXClient jmxClient = getJMXClient(ip, port);
        ObjectInstance transactionObjectInstance = jmxClient.queryMBeanForOne(transactionObjectName);
        jmxClient.invoke(transactionObjectInstance.getObjectName(), "rollbackTransaction", new Object[] { txKey },
            new String[] { "java.lang.String" });
        jmxClient.close();
    }


    private static JMXClient getJMXClient(String ip, int port) throws JMXClientException {
        JMXClient jmxClient = JMXClient.getJMXClient(ip, port);
        return jmxClient;
    }

    // public static void main(String[] args) throws Exception {
    // String[] preparedTransactions =
    // TransactionUtil.getPreparedTransactions("10.232.102.184", 9999);
    // System.out.println(preparedTransactions);
    //
    // TransactionUtil.commitTransaction("TX:sessionId:11", false,
    // "10.232.102.184", 9999);
    // TransactionUtil.rollbackTransaction("TX:sessionId:11", "10.232.102.184",
    // 9999);
    // }
}