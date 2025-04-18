package com.mapreduce.messaging;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapreduce.common.Task;
import com.rabbitmq.client.Channel;

/**
 * 消息生产者
 */
public class MessageProducer {
    private static final Logger logger = LogManager.getLogger(MessageProducer.class);
    
    private final RabbitMQClient rabbitMQClient;
    private final ObjectMapper objectMapper;
    
    /**
     * 创建消息生产者
     */
    public MessageProducer(RabbitMQClient rabbitMQClient) {
        this.rabbitMQClient = rabbitMQClient;
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * 发送任务消息
     */
    public void sendTask(Task task) throws IOException {
        String routingKey = (task.getTaskType() == com.mapreduce.common.TaskType.MAP) ? "map" : "reduce";
        String message = objectMapper.writeValueAsString(task);
        
        Channel channel = null;
        try {
            channel = rabbitMQClient.createChannel();
            channel.basicPublish(
                rabbitMQClient.getTasksExchange(),
                routingKey,
                null,
                message.getBytes(StandardCharsets.UTF_8)
            );
            
            logger.info("Sent task: {}", task.getTaskId());
        } catch (Exception e) {
            logger.error("Error sending task: {}", task.getTaskId(), e);
            throw new IOException("Error sending task", e);
        } finally {
            closeChannelQuietly(channel);
        }
    }
    
    /**
     * 发送 Map 任务结果
     */
    public void sendMapResult(String taskId, String jobId, List<String> outputFiles) throws IOException {
        sendMapResult(taskId, jobId, outputFiles, null);
    }
    
    /**
     * 发送 Map 任务结果（含Blob URLs）
     */
    public void sendMapResult(String taskId, String jobId, List<String> outputFiles, List<String> blobUrls) throws IOException {
        MapResultMessage mapResult = new MapResultMessage(taskId, jobId, outputFiles, blobUrls);
        String message = objectMapper.writeValueAsString(mapResult);
        
        Channel channel = null;
        try {
            channel = rabbitMQClient.createChannel();
            channel.basicPublish(
                rabbitMQClient.getResultsExchange(),
                "map",
                null,
                message.getBytes(StandardCharsets.UTF_8)
            );
            
            logger.info("Sent map result for task: {}", taskId);
        } catch (Exception e) {
            logger.error("Error sending map result for task: {}", taskId, e);
            throw new IOException("Error sending map result", e);
        } finally {
            closeChannelQuietly(channel);
        }
    }
    
    /**
     * 发送 Reduce 任务结果
     */
    public void sendReduceResult(String taskId, String jobId, String outputFile) throws IOException {
        sendReduceResult(taskId, jobId, outputFile, null);
    }
    
    /**
     * 发送 Reduce 任务结果（含Blob URL）
     */
    public void sendReduceResult(String taskId, String jobId, String outputFile, String blobUrl) throws IOException {
        ReduceResultMessage reduceResult = new ReduceResultMessage(taskId, jobId, outputFile, blobUrl);
        String message = objectMapper.writeValueAsString(reduceResult);
        
        Channel channel = null;
        try {
            channel = rabbitMQClient.createChannel();
            channel.basicPublish(
                rabbitMQClient.getResultsExchange(),
                "reduce",
                null,
                message.getBytes(StandardCharsets.UTF_8)
            );
            
            logger.info("Sent reduce result for task: {}", taskId);
        } catch (Exception e) {
            logger.error("Error sending reduce result for task: {}", taskId, e);
            throw new IOException("Error sending reduce result", e);
        } finally {
            closeChannelQuietly(channel);
        }
    }
    
    /**
     * 发送心跳消息
     */
    public void sendHeartbeat(String workerId, String host) throws IOException {
        HeartbeatMessage heartbeat = new HeartbeatMessage(workerId, host);
        String message = objectMapper.writeValueAsString(heartbeat);
        
        Channel channel = null;
        try {
            channel = rabbitMQClient.createChannel();
            channel.basicPublish(
                rabbitMQClient.getStatusExchange(),
                "",
                null,
                message.getBytes(StandardCharsets.UTF_8)
            );
            
            logger.debug("Sent heartbeat for worker: {}", workerId);
        } catch (Exception e) {
            logger.error("Error sending heartbeat for worker: {}", workerId, e);
            throw new IOException("Error sending heartbeat", e);
        } finally {
            closeChannelQuietly(channel);
        }
    }
    
    /**
     * 安全关闭Channel
     */
    private void closeChannelQuietly(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.warn("Error closing channel", e);
            }
        }
    }
    
    /**
     * Map 结果消息类
     */
    public static class MapResultMessage {
        private String taskId;
        private String jobId;
        private List<String> outputFiles;
        private List<String> blobUrls;
        
        public MapResultMessage() {
        }
        
        public MapResultMessage(String taskId, String jobId, List<String> outputFiles) {
            this(taskId, jobId, outputFiles, null);
        }
        
        public MapResultMessage(String taskId, String jobId, List<String> outputFiles, List<String> blobUrls) {
            this.taskId = taskId;
            this.jobId = jobId;
            this.outputFiles = outputFiles;
            this.blobUrls = blobUrls;
        }
        
        public String getTaskId() {
            return taskId;
        }
        
        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }
        
        public String getJobId() {
            return jobId;
        }
        
        public void setJobId(String jobId) {
            this.jobId = jobId;
        }
        
        public List<String> getOutputFiles() {
            return outputFiles;
        }
        
        public void setOutputFiles(List<String> outputFiles) {
            this.outputFiles = outputFiles;
        }
        
        public List<String> getBlobUrls() {
            return blobUrls;
        }
        
        public void setBlobUrls(List<String> blobUrls) {
            this.blobUrls = blobUrls;
        }
    }
    
    /**
     * Reduce 结果消息类
     */
    public static class ReduceResultMessage {
        private String taskId;
        private String jobId;
        private String outputFile;
        private String blobUrl;
        
        public ReduceResultMessage() {
        }
        
        public ReduceResultMessage(String taskId, String jobId, String outputFile) {
            this(taskId, jobId, outputFile, null);
        }
        
        public ReduceResultMessage(String taskId, String jobId, String outputFile, String blobUrl) {
            this.taskId = taskId;
            this.jobId = jobId;
            this.outputFile = outputFile;
            this.blobUrl = blobUrl;
        }
        
        public String getTaskId() {
            return taskId;
        }
        
        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }
        
        public String getJobId() {
            return jobId;
        }
        
        public void setJobId(String jobId) {
            this.jobId = jobId;
        }
        
        public String getOutputFile() {
            return outputFile;
        }
        
        public void setOutputFile(String outputFile) {
            this.outputFile = outputFile;
        }
        
        public String getBlobUrl() {
            return blobUrl;
        }
        
        public void setBlobUrl(String blobUrl) {
            this.blobUrl = blobUrl;
        }
    }
    
    /**
     * 心跳消息类
     */
    public static class HeartbeatMessage {
        private String workerId;
        private String host;
        private String status; // Add status field
     
        public HeartbeatMessage() {
        }
        
        public HeartbeatMessage(String workerId, String host) {
            this.workerId = workerId;
            this.host = host;
        }
        
        public String getWorkerId() {
            return workerId;
        }
        
        public void setWorkerId(String workerId) {
            this.workerId = workerId;
        }
        
        public String getHost() {
            return host;
        }
        
        public void setHost(String host) {
            this.host = host;
        }
        public void setStatus(String status) { // Add setStatus method
            this.status = status;
        }
    
        public String getStatus() { // Optional: Add getStatus method
            return status;
        }  
    }
}