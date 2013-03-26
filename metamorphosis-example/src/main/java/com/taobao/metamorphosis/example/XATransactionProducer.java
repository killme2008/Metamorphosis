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
package com.taobao.metamorphosis.example;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.XADataSource;
import javax.transaction.TransactionManager;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.XAMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;


/**
 * 发送者参与分布式事务的简单例子，基于atomikos
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-26
 * 
 */
public class XATransactionProducer {

    private static XAMessageSessionFactory getXAMessageSessionFactory() throws Exception {
        return new XAMetaMessageSessionFactory(initMetaConfig());
    }


    private static XADataSource getXADataSource() throws SQLException {
        final MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
        mysqlXADataSource
            .setUrl("jdbc:mysql://10.232.36.83:3306/metamorphosis?characterEncoding=utf8&connectTimeout=1000&autoReconnect=true");
        mysqlXADataSource.setUser("notify");
        mysqlXADataSource.setPassword("notify");
        mysqlXADataSource.setPreparedStatementCacheSize(20);
        return mysqlXADataSource;
    }


    public static void main(final String[] args) throws Exception {
        final TransactionManager tm = new UserTransactionManager();

        final String topic = "meta-test";
        final XAMessageSessionFactory xasf = getXAMessageSessionFactory();
        final XADataSource xads = getXADataSource();

        final XATransactionTemplate template = new XATransactionTemplate(tm, xads, xasf);
        template.publishTopic(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;

        while ((line = readLine(reader)) != null) {
            final String message = line;
            try {
                template.executeCallback(new XACallback() {
                    public Object execute(final Connection conn, final XAMessageProducer producer) throws Exception {
                        final PreparedStatement pstmt = conn.prepareStatement("insert into xa_demo(message) values(?)");
                        // 数据库msg不能为null，可以尝试将这里setString设置null，来观察回滚现象。
                        pstmt.setString(1, message);
                        if (pstmt.executeUpdate() <= 0) {
                            throw new RuntimeException("insert message to mysql failed");
                        }
                        pstmt.close();
                        if (!producer.sendMessage(new Message(topic, message.getBytes())).isSuccess()) {
                            throw new RuntimeException("send message failed");
                        }
                        return null;
                    }
                });

            }
            catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }


    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type message to send:");
        return reader.readLine();
    }

}