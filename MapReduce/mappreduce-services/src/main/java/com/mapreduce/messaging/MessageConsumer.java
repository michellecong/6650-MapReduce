package com.mapreduce.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.Task;

/**
 * 消息消费者
 */
public class MessageConsumer {
    private static final Logger logger = LogManager.getLogger(MessageConsumer.class);
    
    private final RabbitMQClient rabbitMQClient;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final List<Channel> channels;
    private final AtomicInteger messageProcessingCount = new AtomicInteger(0);
    
    /**
     * 创建消息消费者
     */
    public MessageConsumer(RabbitMQClient rabbitMQClient) {
        this.rabbitMQClient = rabbitMQClient;
        this.objectMapper = new ObjectMapper();
        this.executorService = Executors.newCachedThreadPool(new NamingThreadFactory("MessageConsumer"));
        this.channels = new java.util.concurrent.CopyOnWriteArrayList<>();
    }
    
    /**
     * 消费 Map 任务
     */
    public void consumeMapTasks(TaskCallback<Task> callback) throws IOException {
        consumeMapTasks(callback, 1);
    }
    
    /**
     * 消费 Map 任务（带预取限制）
     */
    public void consumeMapTasks(TaskCallback<Task> callback, int prefetchCount) throws IOException {
        Channel channel = rabbitMQClient.createChannel();
        channels.add(channel);
        
        // 设置预取数量，确保任务公平分配
        channel.basicQos(prefetchCount);
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            messageProcessingCount.incrementAndGet();
            
            executorService.submit(() -> {
                try {
                    Task task = objectMapper.readValue(message, Task.class);
                    logger.info("Received map task: {}", task.getTaskId());
                    
                    callback.execute(task);
                    
                    // 确认消息已处理
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (Exception e) {
                    logger.error("Error processing map task", e);
                    try {
                        // 消息处理失败，重新入队
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    } catch (IOException ioException) {
                        logger.error("Error sending NACK", ioException);
                    }
                } finally {
                    messageProcessingCount.decrementAndGet();
                }
            });
        };
        
        channel.basicConsume(ConfigManager.MAP_TASKS_QUEUE, false, deliverCallback, consumerTag -> {});
        
        logger.info("Started consuming map tasks with prefetch count: {}", prefetchCount);
    }
    
    /**
     * 消费 Reduce 任务
     */
    public void consumeReduceTasks(TaskCallback<Task> callback) throws IOException {
        consumeReduceTasks(callback, 1);
    }
    
    /**
     * 消费 Reduce 任务（带预取限制）
     */
    public void consumeReduceTasks(TaskCallback<Task> callback, int prefetchCount) throws IOException {
        Channel channel = rabbitMQClient.createChannel();
        channels.add(channel);
        
        // 设置预取数量，确保任务公平分配
        channel.basicQos(prefetchCount);
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            messageProcessingCount.incrementAndGet();
            
            executorService.submit(() -> {
                try {
                    Task task = objectMapper.readValue(message, Task.class);
                    logger.info("Received reduce task: {}", task.getTaskId());
                    
                    callback.execute(task);
                    
                    // 确认消息已处理
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (Exception e) {
                    logger.error("Error processing reduce task", e);
                    try {
                        // 消息处理失败，重新入队
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    } catch (IOException ioException) {
                        logger.error("Error sending NACK", ioException);
                    }
                } finally {
                    messageProcessingCount.decrementAndGet();
                }
            });
        };
        
        channel.basicConsume(ConfigManager.REDUCE_TASKS_QUEUE, false, deliverCallback, consumerTag -> {});
        
        logger.info("Started consuming reduce tasks with prefetch count: {}", prefetchCount);
    }
    
    /**
     * 消费 Map 结果
     */
    public void consumeMapResults(TaskCallback<MessageProducer.MapResultMessage> callback) throws IOException {
        Channel channel = rabbitMQClient.createChannel();
        channels.add(channel);
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            messageProcessingCount.incrementAndGet();
            
            try {
                MessageProducer.MapResultMessage mapResult = objectMapper.readValue(
                    message, MessageProducer.MapResultMessage.class);
                logger.info("Received map result for task: {}", mapResult.getTaskId());
                
                callback.execute(mapResult);
                
                // 确认消息已处理
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                logger.error("Error processing map result", e);
                // 消息处理失败，重新入队
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            } finally {
                messageProcessingCount.decrementAndGet();
            }
        };
        
        channel.basicConsume(ConfigManager.MAP_RESULTS_QUEUE, false, deliverCallback, consumerTag -> {});
        
        logger.info("Started consuming map results");
    }
    
    /**
     * 消费 Reduce 结果
     */
    public void consumeReduceResults(TaskCallback<MessageProducer.ReduceResultMessage> callback) throws IOException {
        Channel channel = rabbitMQClient.createChannel();
        channels.add(channel);
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            messageProcessingCount.incrementAndGet();
            try {
                MessageProducer.ReduceResultMessage reduceResult = objectMapper.readValue(
                    message, MessageProducer.ReduceResultMessage.class);
                logger.info("Received reduce result for task: {}", reduceResult.getTaskId());
                
                callback.execute(reduceResult);
                
                // 确认消息已处理
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                logger.error("Error processing reduce result", e);
                // 消息处理失败，重新入队
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            } finally {
                messageProcessingCount.decrementAndGet();
            }
        };
        
        channel.basicConsume(ConfigManager.REDUCE_RESULTS_QUEUE, false, deliverCallback, consumerTag -> {});
        
        logger.info("Started consuming reduce results");
    }
    
    /**
     * 消费心跳和状态消息
     */
    public void consumeStatusMessages(TaskCallback<MessageProducer.HeartbeatMessage> callback) throws IOException {
        Channel channel = rabbitMQClient.createChannel();
        channels.add(channel);
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            messageProcessingCount.incrementAndGet();
            
            try {
                MessageProducer.HeartbeatMessage heartbeat = objectMapper.readValue(
                    message, MessageProducer.HeartbeatMessage.class);
                
                callback.execute(heartbeat);
                
                // 确认消息已处理
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                logger.error("Error processing status message", e);
                // 状态消息处理失败，但不重新入队
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } finally {
                messageProcessingCount.decrementAndGet();
            }
        };
        
        channel.basicConsume(ConfigManager.STATUS_QUEUE, false, deliverCallback, consumerTag -> {});
        
        logger.info("Started consuming status messages");
    }
    
    /**
     * 关闭消费者
     */
    public void close() {
        logger.info("Shutting down message consumer");
        
        // 等待所有消息处理完成
        while (messageProcessingCount.get() > 0) {
            logger.info("Waiting for {} messages to be processed", messageProcessingCount.get());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        // 关闭所有channels
        for (Channel channel : channels) {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing channel", e);
            }
        }
        channels.clear();
        
        // 关闭线程池
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Message consumer shutdown completed");
    }
    
    /**
     * 获取当前正在处理的消息数量
     */
    public int getMessageProcessingCount() {
        return messageProcessingCount.get();
    }
    
    /**
     * 任务回调接口
     */
    public interface TaskCallback<T> {
        void execute(T data) throws Exception;
    }
    
    /**
     * 自定义线程工厂，用于命名线程
     */
    private static class NamingThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        
        NamingThreadFactory(String prefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = prefix + "-thread-";
        }
        
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}