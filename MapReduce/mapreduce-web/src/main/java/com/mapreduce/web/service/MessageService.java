package com.mapreduce.web.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapreduce.web.config.AppConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Service for sending messages to RabbitMQ
 */
public class MessageService {
    private static final Logger logger = LogManager.getLogger(MessageService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Send job request message to RabbitMQ
     */
    public static void sendJobRequest(String jobId, String inputFile, String blobUrl, int numReduceTasks) 
            throws IOException, TimeoutException {
        Map<String, Object> message = new HashMap<>();
        message.put("jobId", jobId);
        message.put("inputFile", inputFile);
        message.put("blobUrl", blobUrl);
        message.put("numReduceTasks", numReduceTasks);
        message.put("timestamp", System.currentTimeMillis());
        
        String messageJson = objectMapper.writeValueAsString(message);
        
        sendMessage(AppConfig.MQ_EXCHANGE, AppConfig.MQ_JOB_QUEUE, messageJson);
        
        logger.info("Sent job request message for job: {}", jobId);
    }
    
    /**
     * Send message to RabbitMQ
     */
    private static void sendMessage(String exchange, String routingKey, String message) 
            throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(AppConfig.MQ_HOST);
        factory.setPort(AppConfig.MQ_PORT);
        factory.setUsername(AppConfig.MQ_USER);
        factory.setPassword(AppConfig.MQ_PASSWORD);
        
        Connection connection = null;
        Channel channel = null;
        
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            
            // Ensure exchange exists
            channel.exchangeDeclare(exchange, "direct", true);
            
            // Ensure queue exists and is bound to exchange
            channel.queueDeclare(routingKey, true, false, false, null);
            channel.queueBind(routingKey, exchange, routingKey);
            
            // Publish message
            channel.basicPublish(
                exchange,
                routingKey,
                null,
                message.getBytes(StandardCharsets.UTF_8)
            );
            
            logger.debug("Message sent to exchange: {}, routing key: {}", exchange, routingKey);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    logger.warn("Error closing RabbitMQ channel", e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.warn("Error closing RabbitMQ connection", e);
                }
            }
        }
    }
}