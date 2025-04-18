package com.mapreduce.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Configuration manager for MapReduce system
 */
public class ConfigManager {
    private static final Logger logger = LogManager.getLogger(ConfigManager.class);
    private static final Properties properties = new Properties();
    
    // Default values
    public static final String DEFAULT_DB_HOST = "localhost";
    public static final int DEFAULT_DB_PORT = 3306;
    public static final String DEFAULT_DB_NAME = "mapreduce";
    public static final String DEFAULT_DB_USER = "admin";
    public static final String DEFAULT_DB_PASSWORD = "admin";
    
    // RabbitMQ queue and exchange names
    public static final String MAP_TASKS_QUEUE = "map_tasks_queue";
    public static final String REDUCE_TASKS_QUEUE = "reduce_tasks_queue";
    public static final String MAP_RESULTS_QUEUE = "map_results_queue";
    public static final String REDUCE_RESULTS_QUEUE = "reduce_results_queue";
    public static final String STATUS_QUEUE = "status_queue";
    public static final String JOB_QUEUE = "job_queue"; // Added job queue name
    
    // Storage directory names
    public static final String INPUT_DIR = "input";
    public static final String INTERMEDIATE_DIR = "intermediate";
    public static final String OUTPUT_DIR = "output";
    
    // Worker configuration
    public static final int DEFAULT_MAX_MAP_TASKS_PER_WORKER = 2;
    public static final int DEFAULT_MAX_REDUCE_TASKS_PER_WORKER = 1;
    
    static {
        try {
            // 加载配置文件
            loadProperties();
        } catch (IOException e) {
            logger.error("Error loading configuration", e);
        }
    }
    
    /**
     * 加载配置文件
     */
    private static void loadProperties() throws IOException {
        // 首先尝试从指定的配置文件加载
        InputStream inputStream = ConfigManager.class.getClassLoader().getResourceAsStream("application.properties");
        if (inputStream != null) {
            properties.load(inputStream);
            logger.info("Configuration loaded successfully from application.properties");
            inputStream.close();
        } else {
            logger.warn("application.properties not found in classpath");
            
            // 尝试从备用配置文件加载
            inputStream = ConfigManager.class.getClassLoader().getResourceAsStream("default.properties");
            if (inputStream != null) {
                properties.load(inputStream);
                logger.info("Configuration loaded from default.properties");
                inputStream.close();
            } else {
                logger.error("No configuration file found");
            }
        }
    }
    
    // Database configuration
    public static String getDbHost() {
        return properties.getProperty("db.host", DEFAULT_DB_HOST);
    }
    
    public static int getDbPort() {
        return getIntProperty("db.port", DEFAULT_DB_PORT);
    }
    
    public static String getDbName() {
        return properties.getProperty("db.name", DEFAULT_DB_NAME);
    }
    
    public static String getDbUser() {
        return properties.getProperty("db.user", DEFAULT_DB_USER);
    }
    
    public static String getDbPassword() {
        return properties.getProperty("db.password", DEFAULT_DB_PASSWORD);
    }
    
    public static String getDbUseSSL() {
        return properties.getProperty("db.useSSL", "true");
    }

    public static String getDbAllowPublicKeyRetrieval() {
        return properties.getProperty("db.allowPublicKeyRetrieval", "true");
    }

    public static String getDbServerTimezone() {
        return properties.getProperty("db.serverTimezone", "UTC");
    }

    public static String getDbUseUnicode() {
        return properties.getProperty("db.useUnicode", "true");
    }

    public static String getDbCharacterEncoding() {
        return properties.getProperty("db.characterEncoding", "UTF-8");
    }

    // RabbitMQ configuration
    public static String getRabbitMqHost() {
        return properties.getProperty("rabbitmq.host", "localhost");
    }
    
    public static int getRabbitMqPort() {
        return getIntProperty("rabbitmq.port", 5672);
    }
    
    public static String getRabbitMqUsername() {
        return properties.getProperty("rabbitmq.username", "guest");
    }
    
    public static String getRabbitMqPassword() {
        return properties.getProperty("rabbitmq.password", "guest");
    }
    
    public static String getTasksExchange() {
        return properties.getProperty("rabbitmq.tasks_exchange", "tasks_exchange");
    }
    
    public static String getResultsExchange() {
        return properties.getProperty("rabbitmq.results_exchange", "results_exchange");
    }
    
    public static String getStatusExchange() {
        return properties.getProperty("rabbitmq.status_exchange", "status_exchange");
    }
    
    // Added method to get job queue name
    public static String getJobQueue() {
        return properties.getProperty("rabbitmq.queue", JOB_QUEUE);
    }
    
    // Azure Blob Storage configuration
    public static String getAzureConnectionString() {
        return properties.getProperty("azure.storage.connectionString", "");
    }
    
    public static String getAzureContainerName() {
        return properties.getProperty("azure.storage.containerName", "mapreduce");
    }
    
    public static boolean isAzureStorageEnabled() {
        return getBooleanProperty("azure.storage.enabled", false);
    }
    
    // MapReduce configuration
    public static String getBaseDir() {
        return properties.getProperty("mapreduce.baseDir", "mapreduce_data");
    }
    
    public static int getDefaultNumReduceTasks() {
        return getIntProperty("mapreduce.default_num_reduce_tasks", 5);
    }
    
    public static int getDefaultChunkSize() {
        return getIntProperty("mapreduce.default_chunk_size", 64 * 1024 * 1024);
    }
    
    public static long getHeartbeatInterval() {
        return getLongProperty("mapreduce.heartbeat_interval", 5000);
    }
    
    public static int getWorkerTimeoutSeconds() {
        return getIntProperty("mapreduce.worker_timeout_seconds", 15);
    }
    
    public static long getTaskTimeout() {
        return getLongProperty("mapreduce.task_timeout", 300000);
    }
    
    public static int getMaxTaskRetries() {
        return getIntProperty("mapreduce.max_task_retries", 3);
    }
    
    public static int getTaskMonitorIntervalSeconds() {
        return getIntProperty("mapreduce.task_monitor_interval_seconds", 10);
    }
    
    // Worker configuration
    public static int getMaxMapTasksPerWorker() {
        return getIntProperty("mapreduce.max_map_tasks_per_worker", DEFAULT_MAX_MAP_TASKS_PER_WORKER);
    }

    public static int getMaxReduceTasksPerWorker() {
        return getIntProperty("mapreduce.max_reduce_tasks_per_worker", DEFAULT_MAX_REDUCE_TASKS_PER_WORKER);
    }

    /**
     * 重新加载配置
     */
    public static void reload() {
        try {
            properties.clear();
            loadProperties();
            logger.info("Configuration reloaded successfully");
        } catch (IOException e) {
            logger.error("Error reloading configuration", e);
        }
    }

    // Helper methods
    private static int getIntProperty(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Failed to parse integer property: {}", key, e);
            }
        }
        return defaultValue;
    }

    private static long getLongProperty(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                logger.warn("Failed to parse long property: {}", key, e);
            }
        }
        return defaultValue;
    }

    private static boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }
    /**
     * 检查是否只使用Blob存储（禁用本地文件系统）
     */
    public static boolean useOnlyBlobStorage() {
        return getBooleanProperty("mapreduce.use_only_blob_storage", true);
    }

    /**
     * 获取临时目录路径
     */
    public static String getTempDirectory() {
        return getBaseDir() + "/temp";
}
}

