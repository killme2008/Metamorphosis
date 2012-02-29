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
package com.taobao.metamorphosis.tools.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.client.consumer.storage.JDBCUtils;
import com.taobao.metamorphosis.client.consumer.storage.MysqlOffsetStorage;
import com.taobao.metamorphosis.tools.utils.StringUtil;

public class MysqlOffsetStorageQuery implements OffsetStorageQuery {

	Connection connect = null;

	public MysqlOffsetStorageQuery(Connection connect) {
		this.connect = connect;
	}

	public String getOffset(final OffsetQueryDO queryDO) {
		if (!check(queryDO))
			return null;
//		Long offset = (Long)JDBCUtils.execute(connect, new JDBCUtils.ConnectionCallback() {
		String offset = (String)JDBCUtils.execute(connect, new JDBCUtils.ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException {
                final String selectSQL =
                        "select offset from " + MysqlOffsetStorage.DEFAULT_TABLE_NAME
                                + " where topic=? and partition=? and group_id=?";
                PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
                return JDBCUtils.execute(preparedStatement, new JDBCUtils.PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement pstmt) throws SQLException {
                        pstmt.setString(1, queryDO.getTopic());
                        pstmt.setString(2, queryDO.getPartition());
                        pstmt.setString(3, queryDO.getGroup());
                        ResultSet rs = pstmt.executeQuery();
                        return JDBCUtils.execute(rs, new JDBCUtils.ResultSetCallback() {

                            public Object doInResultSet(ResultSet rs) throws SQLException {
                                if (rs.next()) {
//                                    long offset = rs.getLong(1);
                                	
                                    String offsetStr = rs.getString(1);
                                   	return offsetStr;
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
		if(StringUtils.isBlank(offset))	return null;
		return offset;
	}

	private boolean check(OffsetQueryDO queryDO) {
		if (queryDO == null)
			return false;
		if (StringUtil.empty(queryDO.getGroup()))
			return false;
		if (StringUtil.empty(queryDO.getTopic()))
			return false;
		if (StringUtil.empty(queryDO.getPartition()))
			return false;
		return true;
	}

	//TODO 一下暂时未实现
    public List<String> getConsumerGroups() {
        throw new UnsupportedOperationException("暂未实现");
    }

    public List<String> getPartitionsOf(String group, String topic) {
        throw new UnsupportedOperationException("暂未实现");
    }

    public List<String> getTopicsExistOffset(String group) {
        throw new UnsupportedOperationException("暂未实现");
    }

}