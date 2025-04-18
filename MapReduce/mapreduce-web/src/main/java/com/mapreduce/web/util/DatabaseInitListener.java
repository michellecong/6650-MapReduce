package com.mapreduce.web.util;

import javax.servlet.annotation.WebListener;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@WebListener
/**
 * Database initialization listener for web application lifecycle
 */
public class DatabaseInitListener implements ServletContextListener {
    private static final Logger logger = LogManager.getLogger(DatabaseInitListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        logger.info("Web application starting, initializing database connection...");
        try {
            DatabaseUtil.initialize();
            logger.info("Database connection initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize database connection", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("Web application shutting down, closing database connection...");
        try {
            DatabaseUtil.close();
            logger.info("Database connection closed successfully");
        } catch (Exception e) {
            logger.error("Error closing database connection", e);
        }
    }
}