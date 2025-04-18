package com.mapreduce.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Database configuration class, using HikariCP connection pool
 */
public class DatabaseConfig {
    private static final Logger logger = LogManager.getLogger(DatabaseConfig.class);
    
    private static HikariDataSource dataSource;
    
    /**
     * Initialize database connection pool
     */
    public static synchronized void initialize() {
        if (dataSource != null && !dataSource.isClosed()) {
            logger.info("Database connection pool already initialized");
            return;
        }
        
        String host = ConfigManager.getDbHost();
        int port = ConfigManager.getDbPort();
        String database = ConfigManager.getDbName();
        String username = ConfigManager.getDbUser();
        String password = ConfigManager.getDbPassword();
        String useSSL = ConfigManager.getDbUseSSL();
        String allowPublicKeyRetrieval = ConfigManager.getDbAllowPublicKeyRetrieval();
        String serverTimezone = ConfigManager.getDbServerTimezone();
        String useUnicode = ConfigManager.getDbUseUnicode();
        String characterEncoding = ConfigManager.getDbCharacterEncoding();

        try {
            logger.info("Initializing database connection pool: {}:{}/{}", host, port, database);
            
            HikariConfig config = new HikariConfig();
            // config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database +
            //         "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8");
            String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database +
                    "?useSSL=" + useSSL +
                    "&allowPublicKeyRetrieval=" + allowPublicKeyRetrieval +
                    "&serverTimezone=" + serverTimezone +
                    "&useUnicode=" + useUnicode +
                    "&characterEncoding=" + characterEncoding;

            config.setJdbcUrl(jdbcUrl);               
            config.setUsername(username);
            config.setPassword(password);
            
            // Connection pool configuration
            config.setMaximumPoolSize(20);                 // Maximum connections
            config.setMinimumIdle(5);                      // Minimum idle connections
            config.setIdleTimeout(60000);                  // Idle connection timeout (ms)
            config.setConnectionTimeout(30000);            // Connection timeout (ms)
            config.setMaxLifetime(1800000);                // Connection maximum lifetime (ms)
            config.setAutoCommit(true);                    // Auto-commit
            
            // Connection test query
            config.setConnectionTestQuery("SELECT 1");
            
            // Leak detection
            config.setLeakDetectionThreshold(60000);       // Connection leak detection threshold (ms)
            
            // Pool name
            config.setPoolName("MapReduceHikariPool");
            
            // Register JMX monitoring
            config.setRegisterMbeans(true);
            
            dataSource = new HikariDataSource(config);
            logger.info("Database connection pool initialized successfully with {} max connections", config.getMaximumPoolSize());
        } catch (Exception e) {
            logger.error("Failed to initialize database connection pool", e);
            throw new RuntimeException("Failed to initialize database connection pool", e);
        }
    }
    
    /**
     * Get database connection
     */
    public static Connection getConnection() throws SQLException {
        if (dataSource == null || dataSource.isClosed()) {
            throw new SQLException("Database connection pool not initialized or closed");
        }
        
        try {
            Connection conn = dataSource.getConnection();
            if (conn == null) {
                throw new SQLException("Failed to get database connection from pool");
            }
            return conn;
        } catch (SQLException e) {
            logger.error("Error getting database connection", e);
            throw e;
        }
    }
    
    /**
     * Close database connection pool
     */
    public static synchronized void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            try {
                dataSource.close();
                logger.info("Database connection pool closed");
            } catch (Exception e) {
                logger.error("Error closing database connection pool", e);
            }
        }
    }
    
    /**
     * Check connection pool status
     */
    public static boolean isInitialized() {
        return dataSource != null && !dataSource.isClosed();
    }
    
    /**
     * Get connection pool statistics
     */
    public static String getPoolStats() {
        if (dataSource == null || dataSource.isClosed()) {
            return "Connection pool not initialized or closed";
        }
        
        return String.format(
            "Pool stats: Active=%d, Idle=%d, Waiting=%d, Total=%d",
            dataSource.getHikariPoolMXBean().getActiveConnections(),
            dataSource.getHikariPoolMXBean().getIdleConnections(),
            dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection(),
            dataSource.getHikariPoolMXBean().getTotalConnections()
        );
    }
}