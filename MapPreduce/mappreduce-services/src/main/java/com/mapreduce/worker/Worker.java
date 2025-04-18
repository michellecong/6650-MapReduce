package com.mapreduce.worker;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.Task;
import com.mapreduce.common.TaskType;
import com.mapreduce.db.DatabaseConfig;
import com.mapreduce.messaging.MessageConsumer;
import com.mapreduce.messaging.MessageProducer;
import com.mapreduce.messaging.RabbitMQClient;
import com.mapreduce.storage.BlobStorageService;

/**
 * Worker node main class
 */
public class Worker {
    private static final Logger logger = LogManager.getLogger(Worker.class);
    
    private final String workerId;
    private final String host;
    private final RabbitMQClient rabbitMQClient;
    private final MessageProducer messageProducer;
    private final MessageConsumer messageConsumer;
    private final MapWorker mapWorker;
    private final ReduceWorker reduceWorker;
    private final ScheduledExecutorService scheduler;
    private final BlobStorageService blobStorageService;
    
    private volatile boolean running = false;
    
    /**
     * Create Worker node using configuration file
     */
    public Worker() throws Exception {
        // 获取工作节点ID（使用持久化ID或创建新ID）
        this.workerId = getOrCreateWorkerId();
        
        // Get local hostname
        this.host = InetAddress.getLocalHost().getHostName();
        
        // Initialize database connection
        DatabaseConfig.initialize();
        
        // Initialize RabbitMQ client
        this.rabbitMQClient = new RabbitMQClient();
        rabbitMQClient.initialize();
        
        // Initialize Blob storage service
        this.blobStorageService = new BlobStorageService();
        
        // Initialize message producer and consumer
        this.messageProducer = new MessageProducer(rabbitMQClient);
        this.messageConsumer = new MessageConsumer(rabbitMQClient);
        
        // Initialize Map and Reduce task processors
        String baseDir = ConfigManager.getBaseDir();
        int numReduceTasks = ConfigManager.getDefaultNumReduceTasks();
        this.mapWorker = new MapWorker(messageProducer, workerId, numReduceTasks, baseDir, blobStorageService);
        this.reduceWorker = new ReduceWorker(messageProducer, workerId, baseDir, blobStorageService);
        
        // Initialize scheduler with naming thread factory
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "worker-scheduler-" + workerId);
            t.setDaemon(true);
            return t;
        });
        
        logger.info("Worker initialized with ID: {}, host: {}, blobs enabled: {}", 
                  workerId, host, blobStorageService.isEnabled());
    }
    
    /**
     * Create Worker node with custom parameters
     */
    public Worker(String rabbitmqHost, int rabbitmqPort, String rabbitmqUsername, String rabbitmqPassword,
                 String dbHost, int dbPort, String dbName, String dbUsername, String dbPassword,
                 String baseDir, int numReduceTasks) throws Exception {
        
        // 获取工作节点ID（使用持久化ID或创建新ID）
        this.workerId = getOrCreateWorkerId();
        
        // Get local hostname
        this.host = InetAddress.getLocalHost().getHostName();
        
        // Initialize database connection
        DatabaseConfig.initialize();
        
        // Initialize RabbitMQ client
        this.rabbitMQClient = new RabbitMQClient();
        rabbitMQClient.initialize();
        
        // Initialize Blob storage service
        this.blobStorageService = new BlobStorageService();
        
        // Initialize message producer and consumer
        this.messageProducer = new MessageProducer(rabbitMQClient);
        this.messageConsumer = new MessageConsumer(rabbitMQClient);
        
        // Initialize Map and Reduce task processors
        this.mapWorker = new MapWorker(messageProducer, workerId, numReduceTasks, baseDir, blobStorageService);
        this.reduceWorker = new ReduceWorker(messageProducer, workerId, baseDir, blobStorageService);
        
        // Initialize scheduler with naming thread factory
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "worker-scheduler-" + workerId);
            t.setDaemon(true);
            return t;
        });
        
        logger.info("Worker initialized with ID: {}, host: {}, blobs enabled: {}", 
                  workerId, host, blobStorageService.isEnabled());
    }
    
    /**
     * 获取或创建Worker ID
     */
    private String getOrCreateWorkerId() throws IOException {
        // Worker ID 存储文件路径
        String idFilePath = ConfigManager.getBaseDir() + "/worker_id.txt";
        File idFile = new File(idFilePath);
        
        // 尝试读取已有的worker ID
        if (idFile.exists()) {
            try {
                String savedId = new String(Files.readAllBytes(Paths.get(idFilePath))).trim();
                if (!savedId.isEmpty()) {
                    logger.info("Using existing worker ID: {}", savedId);
                    return savedId;
                }
            } catch (IOException e) {
                logger.warn("Could not read worker ID from file: {}", e.getMessage());
            }
        }
        
        // 创建新的 worker ID
        String newId = "worker_" + UUID.randomUUID().toString().substring(0, 8);
        
        // 确保目录存在
        File baseDir = new File(ConfigManager.getBaseDir());
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
        
        // 保存ID到文件
        try {
            Files.write(Paths.get(idFilePath), newId.getBytes());
            logger.info("Generated and saved new worker ID: {}", newId);
        } catch (IOException e) {
            logger.warn("Could not save worker ID to file: {}", e.getMessage());
        }
        
        return newId;
    }
    
    /**
     * Start Worker node
     */
    public void start() throws IOException {
        if (running) {
            logger.warn("Worker already running");
            return;
        }
        
        running = true;
        logger.info("Starting Worker node: {}", workerId);
        
        // 限制每个Worker的任务处理数量，避免资源过度分配
        // Consume Map tasks
        messageConsumer.consumeMapTasks(task -> {
            executeTask(task);
        }, 2);  // 每次只获取2个任务
        
        // Consume Reduce tasks
        messageConsumer.consumeReduceTasks(task -> {
            executeTask(task);
        }, 1);  // 每次只获取1个任务
        
        // Start heartbeat sending
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (running) {
                    messageProducer.sendHeartbeat(workerId, host);
                }
            } catch (IOException e) {
                logger.error("Error sending heartbeat", e);
            }
        }, 0, ConfigManager.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
        
        logger.info("Worker node started successfully");
    }
    
    /**
     * Execute task
     */
    private void executeTask(Task task) throws Exception {
        long startTime = System.currentTimeMillis();
        
        try {
            if (task.getTaskType() == TaskType.MAP) {
                logger.info("Worker {} executing map task: {}", workerId, task.getTaskId());
                mapWorker.execute(task);
            } else if (task.getTaskType() == TaskType.REDUCE) {
                logger.info("Worker {} executing reduce task: {}", workerId, task.getTaskId());
                reduceWorker.execute(task);
            }
            
            long executionTime = System.currentTimeMillis() - startTime;
            logger.info("Task {} completed in {}ms", task.getTaskId(), executionTime);
        } catch (Exception e) {
            logger.error("Error executing task: {}", task.getTaskId(), e);
            throw e;
        }
    }
    
    /**
     * Stop Worker node
     */
    public void stop() {
        if (!running) {
            logger.warn("Worker already stopped");
            return;
        }
        
        logger.info("Stopping Worker node: {}", workerId);
        running = false;
        
        // Stop scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Close message consumer
        messageConsumer.close();
        
        // Close RabbitMQ connection
        rabbitMQClient.close();
        
        // Close database connection
        DatabaseConfig.close();
        
        logger.info("Worker node stopped");
    }
    
    /**
     * Main method
     */
    public static void main(String[] args) {
        Worker worker = null;
        
        try {
            // Parse command line arguments
            CommandLine cmd = parseCommandLine(args);
            
            if (cmd.hasOption("rabbitmq-host") || cmd.hasOption("db-host")) {
                // RabbitMQ configuration
                String rabbitmqHost = cmd.getOptionValue("rabbitmq-host", ConfigManager.getRabbitMqHost());
                int rabbitmqPort = Integer.parseInt(cmd.getOptionValue("rabbitmq-port", 
                                                   String.valueOf(ConfigManager.getRabbitMqPort())));
                String rabbitmqUsername = cmd.getOptionValue("rabbitmq-username", ConfigManager.getRabbitMqUsername());
                String rabbitmqPassword = cmd.getOptionValue("rabbitmq-password", ConfigManager.getRabbitMqPassword());
                
                // Database configuration
                String dbHost = cmd.getOptionValue("db-host", ConfigManager.getDbHost());
                int dbPort = Integer.parseInt(cmd.getOptionValue("db-port", 
                                              String.valueOf(ConfigManager.getDbPort())));
                String dbName = cmd.getOptionValue("db-name", ConfigManager.getDbName());
                String dbUsername = cmd.getOptionValue("db-username", ConfigManager.getDbUser());
                String dbPassword = cmd.getOptionValue("db-password", ConfigManager.getDbPassword());
                
                // Storage configuration
                String baseDir = cmd.getOptionValue("dir", ConfigManager.getBaseDir());
                
                // Reduce task count
                int numReduceTasks = Integer.parseInt(cmd.getOptionValue("reduce", 
                                                   String.valueOf(ConfigManager.getDefaultNumReduceTasks())));
                
                // Create and start Worker
                worker = new Worker(
                    rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword,
                    dbHost, dbPort, dbName, dbUsername, dbPassword,
                    baseDir, numReduceTasks
                );
            } else {
                // Use configuration from file
                worker = new Worker();
            }
            
            worker.start();
            
            // Create CountDownLatch to prevent program exit
            CountDownLatch latch = new CountDownLatch(1);
            
            // Add shutdown hook
            final Worker finalWorker = worker;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown hook triggered, stopping worker...");
                finalWorker.stop();
                latch.countDown();
                logger.info("Shutdown completed");
            }));
            
            // Wait for shutdown signal
            logger.info("Worker running, press Ctrl+C to stop");
            latch.await();
            
        } catch (ParseException e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());
            printHelp();
        } catch (Exception e) {
            logger.error("Error starting Worker", e);
            
            // Try to stop Worker
            if (worker != null) {
                try {
                    worker.stop();
                } catch (Exception ex) {
                    logger.error("Error stopping Worker after startup failure", ex);
                }
            }
            
            System.exit(1);
        }
    }
    
    /**
     * Parse command line arguments
     */
    private static CommandLine parseCommandLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("rh", "rabbitmq-host", true, "RabbitMQ host");
        options.addOption("rp", "rabbitmq-port", true, "RabbitMQ port");
        options.addOption("ru", "rabbitmq-username", true, "RabbitMQ username");
        options.addOption("rpw", "rabbitmq-password", true, "RabbitMQ password");
        options.addOption("dh", "db-host", true, "Database host");
        options.addOption("dp", "db-port", true, "Database port");
        options.addOption("dn", "db-name", true, "Database name");
        options.addOption("du", "db-username", true, "Database username");
        options.addOption("dpw", "db-password", true, "Database password");
        options.addOption("d", "dir", true, "Base directory for storage");
        options.addOption("r", "reduce", true, "Number of reduce tasks");
        
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }
    
    /**
     * Print help information
     */
    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Worker", getOptions());
    }
    
    /**
     * Get command line options
     */
    private static Options getOptions() {
        Options options = new Options();
        options.addOption("rh", "rabbitmq-host", true, "RabbitMQ host (default: " + ConfigManager.getRabbitMqHost() + ")");
        options.addOption("rp", "rabbitmq-port", true, "RabbitMQ port (default: " + ConfigManager.getRabbitMqPort() + ")");
        options.addOption("ru", "rabbitmq-username", true, "RabbitMQ username (default: " + ConfigManager.getRabbitMqUsername() + ")");
        options.addOption("rpw", "rabbitmq-password", true, "RabbitMQ password");
        options.addOption("dh", "db-host", true, "Database host (default: " + ConfigManager.getDbHost() + ")");
        options.addOption("dp", "db-port", true, "Database port (default: " + ConfigManager.getDbPort() + ")");
        options.addOption("dn", "db-name", true, "Database name (default: " + ConfigManager.getDbName() + ")");
        options.addOption("du", "db-username", true, "Database username (default: " + ConfigManager.getDbUser() + ")");
        options.addOption("dpw", "db-password", true, "Database password");
        options.addOption("d", "dir", true, "Base directory for storage (default: " + ConfigManager.getBaseDir() + ")");
        options.addOption("r", "reduce", true, "Number of reduce tasks (default: " + ConfigManager.getDefaultNumReduceTasks() + ")");
        return options;
    }
}