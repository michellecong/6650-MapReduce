package com.mapreduce.web.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Application configuration loader
 */
public class AppConfig {
    private static final Logger logger = LogManager.getLogger(AppConfig.class);
    private static final Properties properties = new Properties();

    static {
        try {
            // Load configuration file
            InputStream inputStream = AppConfig.class.getClassLoader().getResourceAsStream("config.properties");
            if (inputStream != null) {
                properties.load(inputStream);
                logger.info("Configuration loaded successfully");
            } else {
                logger.error("Unable to find config.properties");
            }
        } catch (IOException e) {
            logger.error("Error loading configuration", e);
        }
    }

    // Database configuration
    public static final String DB_HOST = getProperty("db.host", "localhost");
    public static final int DB_PORT = getIntProperty("db.port", 3306);
    public static final String DB_NAME = getProperty("db.name", "mapreduce");
    public static final String DB_USER = getProperty("db.user", "admin");
    public static final String DB_PASSWORD = getProperty("db.password", "admin");
    
    // RabbitMQ configuration
    public static final String MQ_HOST = getProperty("rabbitmq.host", "localhost");
    public static final int MQ_PORT = getIntProperty("rabbitmq.port", 5672);
    public static final String MQ_USER = getProperty("rabbitmq.username", "guest");
    public static final String MQ_PASSWORD = getProperty("rabbitmq.password", "guest");
    public static final String MQ_EXCHANGE = getProperty("rabbitmq.exchange", "tasks_exchange");
    public static final String MQ_JOB_QUEUE = getProperty("rabbitmq.jobQueue", "job_queue");

    public static final String DB_USE_SSL = getProperty("db.useSSL", "true");
    public static final String DB_ALLOW_PUBLIC_KEY_RETRIEVAL = getProperty("db.allowPublicKeyRetrieval", "true");
    public static final String DB_SERVER_TIMEZONE = getProperty("db.serverTimezone", "UTC");
    public static final String DB_USE_UNICODE = getProperty("db.useUnicode", "true");
    public static final String DB_CHARACTER_ENCODING = getProperty("db.characterEncoding", "UTF-8");

    // Azure Blob Storage configuration
    public static final String AZURE_CONNECTION_STRING = getProperty("azure.storage.connectionString", "");
    public static final String AZURE_CONTAINER_NAME = getProperty("azure.storage.containerName", "mapreduce");

    /**
     * Get string property
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * Get string property with default value
     */
    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    /**
     * Get integer property
     */
    public static int getIntProperty(String key, int defaultValue) {
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

    /**
     * Get boolean property
     */
    public static boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }
}