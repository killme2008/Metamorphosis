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
package com.taobao.metamorphosis.client.consumer.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import javax.sql.DataSource;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 基于mysql数据库的offset存储器
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
// TODO 实现自动建表？
public class MysqlOffsetStorage implements OffsetStorage {

    public static final String DEFAULT_TABLE_NAME = "meta_topic_partition_group_offset";

    private DataSource dataSource;

    private String tableName = DEFAULT_TABLE_NAME;


    /**
     * offset保存的表名
     * 
     * @return
     */
    public String getTableName() {
        return this.tableName;
    }


    /**
     * 设置表名，默认为meta_topic_partiton_group_offset
     * 
     * @param tableName
     */
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }


    public MysqlOffsetStorage(final DataSource dataSource) {
        super();
        this.dataSource = dataSource;
    }


    @Override
    public void commitOffset(final String group, final Collection<TopicPartitionRegInfo> infoList) {
        if (infoList == null || infoList.isEmpty()) {
            return;
        }
        final Connection conn = JDBCUtils.getConnection(this.dataSource);

        JDBCUtils.execute(conn, new JDBCUtils.ConnectionCallback() {
            @Override
            public Object doInConnection(final Connection conn) throws SQLException {
                final String updateSQL =
                        "update " + MysqlOffsetStorage.this.tableName
                                + " set offset=?,msg_id=? where topic=? and  partition=?  and group_id=?";
                final PreparedStatement preparedStatement = conn.prepareStatement(updateSQL);
                JDBCUtils.execute(preparedStatement, new JDBCUtils.PreparedStatementCallback() {
                    @Override
                    public Object doInPreparedStatement(final PreparedStatement pstmt) throws SQLException {
                        for (final TopicPartitionRegInfo info : infoList) {
                            long newOffset = -1;
                            long msgId = -1;
                            // 加锁，保证msgId和offset一致
                            synchronized (info) {
                                // 只更新有变更的
                                if (!info.isModified()) {
                                    continue;
                                }
                                newOffset = info.getOffset().get();
                                msgId = info.getMessageId();
                                // 更新完毕，设置为false
                                info.setModified(false);
                            }
                            pstmt.setLong(1, newOffset);
                            pstmt.setLong(2, msgId);
                            pstmt.setString(3, info.getTopic());
                            pstmt.setString(4, info.getPartition().toString());
                            pstmt.setString(5, group);
                            pstmt.addBatch();
                        }
                        pstmt.executeBatch();
                        return null;
                    }
                });
                return null;
            }
        });
    }


    @Override
    public void close() {
        this.dataSource = null;
    }


    @Override
    public void initOffset(final String topic, final String group, final Partition partition, final long offset) {
        final Connection conn = JDBCUtils.getConnection(this.dataSource);
        JDBCUtils.execute(conn, new JDBCUtils.ConnectionCallback() {
            @Override
            public Object doInConnection(final Connection conn) throws SQLException {
                final String insertSQL =
                        "insert into " + MysqlOffsetStorage.this.tableName
                                + " (topic,partition,group_id,offset,msg_id) values(?,?,?,?,?)";
                final PreparedStatement preparedStatement = conn.prepareStatement(insertSQL);
                JDBCUtils.execute(preparedStatement, new JDBCUtils.PreparedStatementCallback() {
                    @Override
                    public Object doInPreparedStatement(final PreparedStatement pstmt) throws SQLException {
                        pstmt.setString(1, topic);
                        pstmt.setString(2, partition.toString());
                        pstmt.setString(3, group);
                        pstmt.setLong(4, offset);
                        pstmt.setLong(5, -1L);
                        pstmt.executeUpdate();
                        return null;
                    }
                });
                return null;
            }
        });
    }


    @Override
    public TopicPartitionRegInfo load(final String topic, final String group, final Partition partition) {
        final Connection conn = JDBCUtils.getConnection(this.dataSource);
        return (TopicPartitionRegInfo) JDBCUtils.execute(conn, new JDBCUtils.ConnectionCallback() {
            @Override
            public Object doInConnection(final Connection conn) throws SQLException {
                final String selectSQL =
                        "select offset,msg_id from " + MysqlOffsetStorage.this.tableName
                                + " where topic=? and partition=? and group_id=?";
                final PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
                return JDBCUtils.execute(preparedStatement, new JDBCUtils.PreparedStatementCallback() {
                    @Override
                    public Object doInPreparedStatement(final PreparedStatement pstmt) throws SQLException {
                        pstmt.setString(1, topic);
                        pstmt.setString(2, partition.toString());
                        pstmt.setString(3, group);
                        final ResultSet rs = pstmt.executeQuery();
                        return JDBCUtils.execute(rs, new JDBCUtils.ResultSetCallback() {

                            @Override
                            public Object doInResultSet(final ResultSet rs) throws SQLException {
                                if (rs.next()) {
                                    final long offset = rs.getLong(1);
                                    final long msgId = rs.getLong(2);
                                    return new TopicPartitionRegInfo(topic, partition, offset, msgId);
                                }
                                else {
                                    return null;
                                }
                            }
                        });
                    }
                });

            }
        });

    }

}