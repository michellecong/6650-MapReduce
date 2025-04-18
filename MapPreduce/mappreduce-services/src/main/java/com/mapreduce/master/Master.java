package com.mapreduce.master;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.JobStatus;
import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.common.TaskType;
import com.mapreduce.db.DatabaseConfig;
import com.mapreduce.db.JobDao;
import com.mapreduce.db.TaskDao;
import com.mapreduce.messaging.MessageConsumer;
import com.mapreduce.messaging.MessageProducer;
import com.mapreduce.messaging.RabbitMQClient;
import com.mapreduce.storage.BlobStorageService;
import com.mapreduce.storage.StorageManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

/**
 * Master node main class
 */
public class Master {
    private static final Logger logger = LogManager.getLogger(Master.class);
    
    private final RabbitMQClient rabbitMQClient;
    private final MessageProducer messageProducer;
    private final MessageConsumer messageConsumer;
    private final StorageManager storageManager;
    private final TaskScheduler taskScheduler;
    private final FilePartitioner filePartitioner;
    private final TaskMonitor taskMonitor;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Channel jobQueueChannel;
    
    /**
     * Create Master node
     */
    public Master() throws Exception {
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
        
        // Initialize storage manager
        String baseDir = ConfigManager.getBaseDir();
        this.storageManager = new StorageManager(baseDir);
        storageManager.initialize();
        
        // Initialize task scheduler
        this.taskScheduler = new TaskScheduler(messageProducer);
        
        // Initialize file partitioner
        this.filePartitioner = new FilePartitioner(taskScheduler, baseDir, storageManager, blobStorageService);
        
        // Initialize task monitor
        this.taskMonitor = new TaskMonitor(taskScheduler, messageConsumer, storageManager);
        
        logger.info("Master initialized successfully");
    }
    
    /**
     * Create Master node with custom parameters
     */
    public Master(String rabbitmqHost, int rabbitmqPort, String rabbitmqUsername, String rabbitmqPassword,
                 String dbHost, int dbPort, String dbName, String dbUsername, String dbPassword,
                 String baseDir) throws Exception {
        
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
        
        // Initialize storage manager
        this.storageManager = new StorageManager(baseDir);
        storageManager.initialize();
        
        // Initialize task scheduler
        this.taskScheduler = new TaskScheduler(messageProducer);
        
        // Initialize file partitioner
        this.filePartitioner = new FilePartitioner(taskScheduler, baseDir, storageManager, blobStorageService);
        
        // Initialize task monitor
        this.taskMonitor = new TaskMonitor(taskScheduler, messageConsumer, storageManager);
        
        logger.info("Master initialized successfully");
    }
    
    /**
     * Start Master node
     */
    public void start() throws IOException {
        logger.info("Starting Master node");
        
        // Start task monitor
        taskMonitor.start();
        
        // Start consuming job messages from job_queue
        setupJobQueueConsumer();
        
        logger.info("Master node started successfully");
    }
    
    /**
     * Setup consumer for job_queue to process new job requests
     */
    private void setupJobQueueConsumer() {
        try {
            // Create a dedicated channel for job queue
            jobQueueChannel = rabbitMQClient.createChannel();
            
            // Ensure queue exists
            String queueName = ConfigManager.getJobQueue();
            jobQueueChannel.queueDeclare(queueName, true, false, false, null);
            
            // Set up consumer
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String messageContent = new String(delivery.getBody(), StandardCharsets.UTF_8);
                logger.info("Received job message: {}", messageContent);
                
                try {
                    // Parse message content
                    @SuppressWarnings("unchecked")
                    Map<String, Object> jobMessage = objectMapper.readValue(messageContent, Map.class);
                    
                    // Extract job details
                    String jobId = (String) jobMessage.get("jobId");
                    String inputFile = (String) jobMessage.get("inputFile");
                    String blobUrl = (String) jobMessage.get("blobUrl");
                    int numReduceTasks = ((Number) jobMessage.get("numReduceTasks")).intValue();
                    
                    logger.info("Received job request: jobId={}, inputFile={}, blobUrl={}, numReduceTasks={}", 
                              jobId, inputFile, blobUrl, numReduceTasks);
                    
                    // Process the job - don't create a new job, just update status and process
                    processExistingJob(jobId, inputFile, blobUrl, numReduceTasks);
                    
                    // Acknowledge message
                    jobQueueChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (Exception e) {
                    logger.error("Error processing job message", e);
                    // Reject message and put it back in queue
                    jobQueueChannel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                }
            };
            
            // Start consuming messages, with qos=1 for fair dispatch
            jobQueueChannel.basicQos(1);
            jobQueueChannel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
            
            logger.info("Started consuming job messages from queue: {}", queueName);
        } catch (Exception e) {
            logger.error("Failed to setup job queue consumer", e);
        }
    }
    
    /**
     * Process an existing job that was created by the web API
     */
    private void processExistingJob(String jobId, String inputFile, String blobUrl, int numReduceTasks) 
            throws IOException, SQLException {
        logger.info("Processing existing job: {} with blobUrl: {}", jobId, blobUrl);
        
        // 查询现有的任务，如果存在Map或Reduce任务,则不再创建新的任务
        List<Task> existingMapTasks = TaskDao.getTasksByJobAndType(jobId, TaskType.MAP);
        List<Task> existingReduceTasks = TaskDao.getTasksByJobAndType(jobId, TaskType.REDUCE);
        
        if (!existingMapTasks.isEmpty()) {
            logger.info("Job {} already has {} map tasks, not creating new ones", 
                    jobId, existingMapTasks.size());
        } else {
            // Update job status to RUNNING
            JobDao.updateJobStatus(jobId, JobStatus.RUNNING);
            
            // Split file and create Map tasks
            List<Task> mapTasks = filePartitioner.splitFileAndCreateMapTasks(inputFile, jobId, StorageType.BLOB, blobUrl);
            
            logger.info("Created {} map tasks for job: {}", mapTasks.size(), jobId);
            
            // 等待所有Map任务完成
            logger.info("Waiting for all Map tasks to complete before creating Reduce tasks");
        }
        
        // 不立即创建Reduce任务，让TaskMonitor检测到Map任务完成后再创建
    }
    
    /**
     * Create and start MapReduce job
     */
    public String startJob(String inputFile, int numReduceTasks) throws IOException, SQLException {
        return startJob(inputFile, numReduceTasks, StorageType.LOCAL, null);
    }
    
    /**
     * Create and start MapReduce job (with Blob storage support)
     * This method is for direct command line job submission
     */
    public String startJob(String inputFile, int numReduceTasks, StorageType storageType, String inputBlobUrl) 
            throws IOException, SQLException {
        logger.info("Starting new job for input file: {}, storageType: {}", inputFile, storageType);
        
        // Create job
        String jobId = JobDao.createJob(inputFile, numReduceTasks, storageType, inputBlobUrl);
        
        // Update job status to RUNNING
        JobDao.updateJobStatus(jobId, JobStatus.RUNNING);
        
        // Split file and create Map tasks
        List<Task> mapTasks = filePartitioner.splitFileAndCreateMapTasks(inputFile, jobId, storageType, inputBlobUrl);
        
        logger.info("Created job: {} with {} map tasks", jobId, mapTasks.size());
        
        return jobId;
    }
    
    /**
     * Stop Master node
     */
    public void stop() {
        logger.info("Stopping Master node");
        
        // Close job queue channel if open
        if (jobQueueChannel != null && jobQueueChannel.isOpen()) {
            try {
                jobQueueChannel.close();
            } catch (Exception e) {
                logger.warn("Error closing job queue channel", e);
            }
        }
        
        // Stop task monitor
        taskMonitor.stop();
        
        // Close RabbitMQ connection
        rabbitMQClient.close();
        
        // Close database connection
        DatabaseConfig.close();
        
        logger.info("Master node stopped");
    }
    
    /**
     * Get storage manager
     */
    public StorageManager getStorageManager() {
        return storageManager;
    }
    
    /**
     * Get Blob storage service
     */
    public BlobStorageService getBlobStorageService() {
        return blobStorageService;
    }
    
    /**
     * Main method
     */
    public static void main(String[] args) {
        // Parse command line arguments
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
        options.addOption("i", "input", true, "Input file path");
        options.addOption("r", "reduce", true, "Number of reduce tasks");
        
        CommandLineParser parser = new DefaultParser();
        Master master = null;
        
        try {
            CommandLine cmd = parser.parse(options, args);
            
            if (cmd.hasOption("rabbitmq-host") || cmd.hasOption("db-host")) {
                // Use command line arguments if provided
                String rabbitmqHost = cmd.getOptionValue("rabbitmq-host", ConfigManager.getRabbitMqHost());
                int rabbitmqPort = Integer.parseInt(cmd.getOptionValue("rabbitmq-port", String.valueOf(ConfigManager.getRabbitMqPort())));
                String rabbitmqUsername = cmd.getOptionValue("rabbitmq-username", ConfigManager.getRabbitMqUsername());
                String rabbitmqPassword = cmd.getOptionValue("rabbitmq-password", ConfigManager.getRabbitMqPassword());
                
                String dbHost = cmd.getOptionValue("db-host", ConfigManager.getDbHost());
                int dbPort = Integer.parseInt(cmd.getOptionValue("db-port", String.valueOf(ConfigManager.getDbPort())));
                String dbName = cmd.getOptionValue("db-name", ConfigManager.getDbName());
                String dbUsername = cmd.getOptionValue("db-username", ConfigManager.getDbUser());
                String dbPassword = cmd.getOptionValue("db-password", ConfigManager.getDbPassword());
                
                String baseDir = cmd.getOptionValue("dir", ConfigManager.getBaseDir());
                
                // Create and start Master
                master = new Master(
                    rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword,
                    dbHost, dbPort, dbName, dbUsername, dbPassword,
                    baseDir
                );
            } else {
                // Use configuration from file
                master = new Master();
            }
            
            master.start();
            
            // If input file specified, start job
            if (cmd.hasOption("input")) {
                String inputFile = cmd.getOptionValue("input");
                int numReduceTasks = Integer.parseInt(cmd.getOptionValue("reduce", 
                                                    String.valueOf(ConfigManager.getDefaultNumReduceTasks())));
                
                String jobId = master.startJob(inputFile, numReduceTasks);
                logger.info("Started job with ID: {}", jobId);
            }
            
            // Create CountDownLatch to prevent program exit
            CountDownLatch latch = new CountDownLatch(1);
            
            // Add shutdown hook
            final Master finalMaster = master;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown hook triggered, stopping master...");
                finalMaster.stop();
                latch.countDown();
                logger.info("Shutdown completed");
            }));
            
            // Wait for shutdown signal
            logger.info("Master running, press Ctrl+C to stop");
            latch.await();
            
        } catch (ParseException e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Master", options);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Error starting Master", e);
            
            // Try to stop Master
            if (master != null) {
                try {
                    master.stop();
                } catch (Exception ex) {
                    logger.error("Error stopping Master after startup failure", ex);
                }
            }
            
            System.exit(1);
        }
    }
}