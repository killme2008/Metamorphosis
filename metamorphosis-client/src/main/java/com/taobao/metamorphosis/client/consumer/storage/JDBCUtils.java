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

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * JDBCπ§æﬂ¿‡
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
public class JDBCUtils {
    static final Log log = LogFactory.getLog(JDBCUtils.class);

    public static class CannotGetConnectionException extends RuntimeException {
        static final long serialVersionUID = -1L;


        public CannotGetConnectionException(String message, Throwable cause) {
            super(message, cause);

        }


        public CannotGetConnectionException(String message) {
            super(message);

        }

    }

    public interface ConnectionCallback {
        public Object doInConnection(Connection conn) throws SQLException;
    }

    public interface PreparedStatementCallback {
        public Object doInPreparedStatement(PreparedStatement pstmt) throws SQLException;
    }

    public interface ResultSetCallback {
        public Object doInResultSet(ResultSet rs) throws SQLException;
    }


    public static Connection getConnection(DataSource ds) {
        try {
            return ds.getConnection();
        }
        catch (SQLException e) {
            throw new CannotGetConnectionException("Can not get connection from datasource", e);
        }
    }


    public static Object execute(Connection conn, ConnectionCallback connectionCallBack) {
        try {
            if (connectionCallBack != null && conn != null) {
                return connectionCallBack.doInConnection(conn);
            }
        }
        catch (SQLException e) {
            log.error("doInConnection failed", e);
        }
        finally {
            close(conn);
        }
        return null;
    }


    public static Object execute(PreparedStatement pstmt, PreparedStatementCallback pstmtCallBack) {
        try {
            if (pstmtCallBack != null && pstmt != null) {
                return pstmtCallBack.doInPreparedStatement(pstmt);
            }
        }
        catch (SQLException e) {
            log.error("doInPreparedStatement failed", e);
        }
        finally {
            close(pstmt);
        }
        return null;
    }


    public static Object execute(ResultSet rs, ResultSetCallback rsCallback) {
        try {
            if (rsCallback != null && rs != null) {
                return rsCallback.doInResultSet(rs);
            }
        }
        catch (SQLException e) {
            log.error("doInResultSet failed", e);
        }
        finally {
            close(rs);
        }
        return null;
    }


    public static void close(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            }
            catch (SQLException e) {
                log.error("Close PreparedStatement failed", e);
            }

        }
    }


    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            }
            catch (SQLException ex) {
                log.error("Close ResultSet failed", ex);
            }
        }
    }


    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException ex) {
                log.error("Close connection failed", ex);
            }
        }
    }

}