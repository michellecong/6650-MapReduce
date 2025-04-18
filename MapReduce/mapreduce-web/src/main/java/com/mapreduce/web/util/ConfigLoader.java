package com.mapreduce.web.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 配置加载器，用于读取配置文件
 */
public class ConfigLoader {
    private static final Logger logger = LogManager.getLogger(ConfigLoader.class);
    private static final Properties properties = new Properties();

    static {
        try {
            // 加载配置文件
            InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream("mapreduce.properties");
            if (inputStream != null) {
                properties.load(inputStream);
                logger.info("Configuration loaded successfully");
            } else {
                logger.error("Unable to find mapreduce.properties");
            }
        } catch (IOException e) {
            logger.error("Error loading configuration", e);
        }
    }

    /**
     * 获取字符串属性
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * 获取字符串属性，带默认值
     */
    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    /**
     * 获取整数属性
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
     * 获取布尔属性
     */
    public static boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }
}