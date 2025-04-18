package com.mapreduce.web.util;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.web.config.AppConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Database utility for connection management
 */
public class DatabaseUtil {
    private static final Logger logger = LogManager.getLogger(DatabaseUtil.class);
    private static HikariDataSource dataSource;
    private static boolean initialized = false;
    private static boolean useDirectConnection = false;

    // JDBC连接信息 - 可以根据需要从AppConfig中读取或直接在这里设置
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/mapreduce?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "admin";
    
    static {
        // 确保驱动程序加载
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error("MySQL JDBC Driver not found", e);
        }
    }

    /**
     * Initialize database connection pool
     */
    public static synchronized void initialize() {
        if (initialized) {
            return;
        }

        try {
            String dbUrl;
            if (AppConfig.DB_HOST != null) {
                dbUrl = String.format(
                    "jdbc:mysql://%s:%d/%s?useSSL=%s&allowPublicKeyRetrieval=%s&serverTimezone=%s&useUnicode=%s&characterEncoding=%s",
                    AppConfig.DB_HOST,
                    AppConfig.DB_PORT,
                    AppConfig.DB_NAME,
                    AppConfig.DB_USE_SSL,
                    AppConfig.DB_ALLOW_PUBLIC_KEY_RETRIEVAL,
                    AppConfig.DB_SERVER_TIMEZONE,
                    AppConfig.DB_USE_UNICODE,
                    AppConfig.DB_CHARACTER_ENCODING
                );
            } else {
                dbUrl = JDBC_URL;
            }            
            
            String dbUser = AppConfig.DB_USER != null ? AppConfig.DB_USER : DB_USER;
            String dbPassword = AppConfig.DB_PASSWORD != null ? AppConfig.DB_PASSWORD : DB_PASSWORD;
            
            logger.info("Initializing database connection pool: {}", dbUrl);
            
            // 首先测试直接连接是否可用
            try (Connection testConn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                logger.info("Direct JDBC connection test successful");
            }
            
            // 如果直接连接成功，再尝试设置连接池
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(dbUrl);
            config.setUsername(dbUser);
            config.setPassword(dbPassword);
            
            // Pool configuration
            config.setMaximumPoolSize(10);
            config.setMinimumIdle(2);
            config.setIdleTimeout(60000);
            config.setConnectionTimeout(30000);
            config.setMaxLifetime(1800000);
            
            // Connection test
            config.setConnectionTestQuery("SELECT 1");
            
            // Pool name
            config.setPoolName("MapReduceWebHikariPool");
            
            try {
                dataSource = new HikariDataSource(config);
                // 测试从连接池获取连接
                try (Connection poolConn = dataSource.getConnection()) {
                    logger.info("HikariCP connection pool initialized successfully");
                }
                useDirectConnection = false;
            } catch (Exception e) {
                logger.warn("Failed to initialize connection pool, will use direct connections", e);
                useDirectConnection = true;
            }
            
            initialized = true;
        } catch (Exception e) {
            logger.error("Failed to initialize database connection", e);
            throw new RuntimeException("Failed to initialize database connection", e);
        }
    }
    
    public static boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Get database connection (either from pool or direct)
     */
    public static Connection getConnection() throws SQLException {
        if (!initialized) {
            initialize();
        }
        
        if (useDirectConnection || dataSource == null || dataSource.isClosed()) {
            logger.debug("Getting direct JDBC connection");
            return DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
        } else {
            logger.debug("Getting connection from pool");
            return dataSource.getConnection();
        }
    }

    /**
     * Close connection pool
     */
    public static synchronized void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool closed");
        }
        initialized = false;
    }

    /**
     * Close ResultSet quietly
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
     * Close Statement quietly
     */
    public static void closeQuietly(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.warn("Error closing Statement", e);
            }
        }
    }

    /**
     * Close PreparedStatement quietly
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
     * Close Connection quietly
     */
    public static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    if (!conn.getAutoCommit()) {
                        try {
                            conn.rollback();
                        } catch (SQLException e) {
                            logger.warn("Error rolling back transaction", e);
                        }
                    }
                    conn.close();
                }
            } catch (SQLException e) {
                logger.warn("Error closing Connection", e);
            }
        }
    }

    /**
     * Close all database resources quietly
     */
    public static void closeQuietly(Connection conn, Statement stmt, ResultSet rs) {
        closeQuietly(rs);
        closeQuietly(stmt);
        closeQuietly(conn);
    }
    
    /**
     * 简单测试数据库连接是否正常
     * @return 连接状态信息
     */
    public static String testConnection() {
        StringBuilder result = new StringBuilder();
        try {
            Connection conn = getConnection();
            result.append("Database connection successful!\n");
            
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");
                if (rs.next()) {
                    result.append("Query test successful: ").append(rs.getInt(1)).append("\n");
                }
            }
            
            closeQuietly(conn);
        } catch (Exception e) {
            result.append("Database connection failed: ").append(e.getMessage()).append("\n");
            e.printStackTrace();
        }
        return result.toString();
    }
}