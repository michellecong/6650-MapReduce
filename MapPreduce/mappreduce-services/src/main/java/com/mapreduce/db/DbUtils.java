package com.mapreduce.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 数据库工具类，用于处理数据库资源的安全关闭
 */
public class DbUtils {
    private static final Logger logger = LogManager.getLogger(DbUtils.class);
    
    /**
     * 安全关闭ResultSet
     */
    public static void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.warn("Error closing ResultSet", e);
            }
        }
    }
    
    /**
     * 安全关闭PreparedStatement
     */
    public static void closeQuietly(PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.warn("Error closing PreparedStatement", e);
            }
        }
    }
    
    /**
     * 安全关闭Connection
     */
    public static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.isClosed() && !conn.getAutoCommit()) {
                    try {
                        conn.rollback();
                    } catch (SQLException e) {
                        logger.warn("Error rolling back transaction", e);
                    }
                }
                conn.close();
            } catch (SQLException e) {
                logger.warn("Error closing Connection", e);
            }
        }
    }
    
    /**
     * 安全关闭所有数据库资源
     */
    public static void closeQuietly(Connection conn, PreparedStatement stmt, ResultSet rs) {
        closeQuietly(rs);
        closeQuietly(stmt);
        closeQuietly(conn);
    }
    
    /**
     * 安全关闭PreparedStatement和Connection
     */
    public static void closeQuietly(Connection conn, PreparedStatement stmt) {
        closeQuietly(stmt);
        closeQuietly(conn);
    }
    
    /**
     * 提交事务并关闭连接
     */
    public static void commitAndClose(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.commit();
                }
            } catch (SQLException e) {
                logger.error("Error committing transaction", e);
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    logger.error("Error rolling back transaction", ex);
                }
            } finally {
                closeQuietly(conn);
            }
        }
    }
    
    /**
     * 回滚事务并关闭连接
     */
    public static void rollbackAndClose(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (SQLException e) {
                logger.error("Error rolling back transaction", e);
            } finally {
                closeQuietly(conn);
            }
        }
    }
}