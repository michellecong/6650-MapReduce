package com.mapreduce.messaging;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * RabbitMQ client
 */
public class RabbitMQClient {
    private static final Logger logger = LogManager.getLogger(RabbitMQClient.class);
    
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private Connection connection;
    
    // Queue and exchange names from config
    private final String tasksExchange;
    private final String resultsExchange;
    private final String statusExchange;
    
    /**
     * Create RabbitMQ client
     */
    public RabbitMQClient() {
        // Read configuration from ConfigManager
        this.host = ConfigManager.getRabbitMqHost();
        this.port = ConfigManager.getRabbitMqPort();
        this.username = ConfigManager.getRabbitMqUsername();
        this.password = ConfigManager.getRabbitMqPassword();
        
        // Get exchange names
        this.tasksExchange = ConfigManager.getTasksExchange();
        this.resultsExchange = ConfigManager.getResultsExchange();
        this.statusExchange = ConfigManager.getStatusExchange();
        
        logger.info("Initialized RabbitMQClient with host: {}, port: {}", host, port);
    }
    
    /**
     * Create RabbitMQ client with custom parameters
     */
    public RabbitMQClient(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        
        // Get exchange names from ConfigManager
        this.tasksExchange = ConfigManager.getTasksExchange();
        this.resultsExchange = ConfigManager.getResultsExchange();
        this.statusExchange = ConfigManager.getStatusExchange();
    }
    
    /**
     * Initialize connection and queues
     */
    public void initialize() throws IOException, TimeoutException {
        // Create connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        
        connection = factory.newConnection();
        
        // Create Channel
        Channel channel = connection.createChannel();
        
        // Declare exchanges
        channel.exchangeDeclare(tasksExchange, "direct", true);
        channel.exchangeDeclare(resultsExchange, "direct", true);
        channel.exchangeDeclare(statusExchange, "fanout", true);
        
        // Declare queues
        channel.queueDeclare(ConfigManager.MAP_TASKS_QUEUE, true, false, false, null);
        channel.queueDeclare(ConfigManager.REDUCE_TASKS_QUEUE, true, false, false, null);
        channel.queueDeclare(ConfigManager.MAP_RESULTS_QUEUE, true, false, false, null);
        channel.queueDeclare(ConfigManager.REDUCE_RESULTS_QUEUE, true, false, false, null);
        channel.queueDeclare(ConfigManager.STATUS_QUEUE, true, false, false, null);
        
        // Bind queues to exchanges
        channel.queueBind(ConfigManager.MAP_TASKS_QUEUE, tasksExchange, "map");
        channel.queueBind(ConfigManager.REDUCE_TASKS_QUEUE, tasksExchange, "reduce");
        channel.queueBind(ConfigManager.MAP_RESULTS_QUEUE, resultsExchange, "map");
        channel.queueBind(ConfigManager.REDUCE_RESULTS_QUEUE, resultsExchange, "reduce");
        channel.queueBind(ConfigManager.STATUS_QUEUE, statusExchange, "");
        
        // Close Channel
        channel.close();
        
        logger.info("RabbitMQ client initialized successfully");
    }
    
    /**
     * Create a new channel
     */
    public Channel createChannel() throws IOException {
        if (connection == null || !connection.isOpen()) {
            throw new IOException("RabbitMQ connection is not open");
        }
        return connection.createChannel();
    }
    
    /**
     * Close connection
     */
    public void close() {
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
                logger.info("RabbitMQ connection closed");
            } catch (IOException e) {
                logger.error("Error closing RabbitMQ connection", e);
            }
        }
    }
    
    /**
     * Get tasks exchange name
     */
    public String getTasksExchange() {
        return tasksExchange;
    }
    
    /**
     * Get results exchange name
     */
    public String getResultsExchange() {
        return resultsExchange;
    }
    
    /**
     * Get status exchange name
     */
    public String getStatusExchange() {
        return statusExchange;
    }
}