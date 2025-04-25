package com.singlenode.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.io.InputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * 数据库配置类，负责管理数据库连接池
 */
public class DatabaseConfig {
  private static final Logger logger = LogManager.getLogger(DatabaseConfig.class);
  private static HikariDataSource dataSource;
  private static final Properties properties = new Properties();

  static {
    loadProperties();
  }

  /**
   * 加载配置文件
   */
  private static void loadProperties() {
    try (InputStream input = DatabaseConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input == null) {
        logger.warn("Unable to find application.properties, using default values");
        return;
      }
      properties.load(input);
      logger.info("Loaded database configuration");
    } catch (Exception e) {
      logger.error("Failed to load application.properties", e);
    }
  }

  /**
   * 获取配置属性
   */
  private static String getProperty(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }

  /**
   * 获取整数配置属性
   */
  private static int getIntProperty(String key, int defaultValue) {
    String value = getProperty(key, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * 初始化数据库连接池
   */
  public static synchronized void initialize() {
    if (dataSource != null && !dataSource.isClosed()) {
      logger.info("Database connection pool already initialized");
      return;
    }

    try {
      // 显式加载MySQL驱动
      Class.forName("com.mysql.cj.jdbc.Driver");

      String dbHost = getProperty("db.host", "localhost");
      int dbPort = getIntProperty("db.port", 3306);
      String dbName = getProperty("db.name", "singlenode");
      String dbUser = getProperty("db.user", "admin");
      String dbPassword = getProperty("db.password", "admin");

      logger.info("Initializing database connection pool: {}:{}/{}", dbHost, dbPort, dbName);

      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(String.format(
          "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8",
          dbHost, dbPort, dbName));
      config.setUsername(dbUser);
      config.setPassword(dbPassword);

      // 连接池配置
      config.setMaximumPoolSize(getIntProperty("db.pool.max", 10));
      config.setMinimumIdle(getIntProperty("db.pool.min", 2));
      config.setIdleTimeout(getIntProperty("db.pool.idle.timeout", 60000));
      config.setConnectionTimeout(getIntProperty("db.pool.connection.timeout", 30000));
      config.setMaxLifetime(getIntProperty("db.pool.max.lifetime", 1800000));

      // 连接测试查询
      config.setConnectionTestQuery("SELECT 1");

      // 池名称
      config.setPoolName("SingleNodeHikariPool");

      dataSource = new HikariDataSource(config);

      logger.info("Database connection pool initialized successfully");
    } catch (ClassNotFoundException e) {
      logger.error("MySQL JDBC Driver not found", e);
      throw new RuntimeException("MySQL JDBC Driver not found", e);
    } catch (Exception e) {
      logger.error("Failed to initialize database connection pool", e);
      throw new RuntimeException("Failed to initialize database connection pool", e);
    }
  }

  /**
   * 获取数据库连接
   */
  public static Connection getConnection() throws SQLException {
    if (dataSource == null || dataSource.isClosed()) {
      throw new SQLException("Database connection pool not initialized or closed");
    }

    return dataSource.getConnection();
  }

  /**
   * 关闭连接池
   */
  public static synchronized void close() {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
      logger.info("Database connection pool closed");
    }
  }
}