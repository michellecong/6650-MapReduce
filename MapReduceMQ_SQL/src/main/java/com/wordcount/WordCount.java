package com.wordcount;

import com.rabbitmq.client.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

public class WordCount {
  // Queue names
  private static String MAP_QUEUE;
  private static String REDUCE_QUEUE;
  private static String RESULT_QUEUE;
  private static String MAP_COMPLETE_QUEUE;

  // Database configuration
  private static String DB_URL;
  private static String DB_USER;
  private static String DB_PASSWORD;

  // Batch size constants
  private static int PROGRESS_UPDATE_BATCH; // How many rows to process before updating progress
  private static int WORD_COUNT_BATCH_SIZE; // How many words to accumulate before saving
  private static int DB_BATCH_SIZE; // Database batch size
  private static int REDUCE_SHARDS; // Number of Reduce shards

  // Connection pool
  private static HikariDataSource dataSource;

  // Configuration object
  private static Properties config = new Properties();

  // Load configuration
  static {
    // Try to load configuration from path specified on command line
    String configPath = System.getProperty("config.path", "config.properties");

    try (InputStream input = new FileInputStream(configPath)) {
      config.load(input);
      System.out.println("Loaded configuration from: " + configPath);

      // Initialize queue names
      MAP_QUEUE = config.getProperty("queue.map", "map_queue");
      REDUCE_QUEUE = config.getProperty("queue.reduce", "reduce_queue");
      RESULT_QUEUE = config.getProperty("queue.result", "result_queue");
      MAP_COMPLETE_QUEUE = config.getProperty("queue.map_complete", "map_complete_queue");

      // Initialize database configuration
      DB_URL = config.getProperty("db.url", "jdbc:mysql://localhost:3306/wordcount");
      DB_USER = config.getProperty("db.user", "user");
      DB_PASSWORD = config.getProperty("db.password", "password");

      // Initialize batch processing configuration
      PROGRESS_UPDATE_BATCH = Integer.parseInt(config.getProperty("batch.progress_update", "100"));
      WORD_COUNT_BATCH_SIZE = Integer.parseInt(config.getProperty("batch.word_count", "1000"));
      DB_BATCH_SIZE = Integer.parseInt(config.getProperty("batch.db_size", "500"));
      REDUCE_SHARDS = Integer.parseInt(config.getProperty("reduce.shards", "10"));

      // Initialize connection pool
      initDataSource();

    } catch (IOException e) {
      System.err.println("Warning: Could not load configuration file: " + configPath);
      System.err.println("Error details: " + e.getMessage());
      System.out.println("Using default values");

      // Use default values
      MAP_QUEUE = "map_queue";
      REDUCE_QUEUE = "reduce_queue";
      RESULT_QUEUE = "result_queue";
      MAP_COMPLETE_QUEUE = "map_complete_queue";

      DB_URL = "jdbc:mysql://localhost:3306/wordcount";
      DB_USER = "user";
      DB_PASSWORD = "password";

      PROGRESS_UPDATE_BATCH = 100;
      WORD_COUNT_BATCH_SIZE = 1000;
      DB_BATCH_SIZE = 500;
      REDUCE_SHARDS = 10;

      // Initialize connection pool
      initDataSource();
    }
  }

  // Initialize data source
  private static void initDataSource() {
    try {
      HikariConfig hikariConfig = new HikariConfig();
      hikariConfig.setJdbcUrl(DB_URL);
      hikariConfig.setUsername(DB_USER);
      hikariConfig.setPassword(DB_PASSWORD);
      hikariConfig.setMaximumPoolSize(Integer.parseInt(config.getProperty("db.pool.maxSize", "10")));
      hikariConfig.setMinimumIdle(Integer.parseInt(config.getProperty("db.pool.minIdle", "5")));
      hikariConfig.setIdleTimeout(Long.parseLong(config.getProperty("db.pool.idleTimeout", "30000")));
      hikariConfig.setMaxLifetime(Long.parseLong(config.getProperty("db.pool.maxLifetime", "1800000")));
      hikariConfig.setConnectionTimeout(Long.parseLong(config.getProperty("db.pool.connectionTimeout", "30000")));
      hikariConfig.addDataSourceProperty("cachePrepStmts", config.getProperty("db.props.cachePrepStmts", "true"));
      hikariConfig.addDataSourceProperty("prepStmtCacheSize", config.getProperty("db.props.prepStmtCacheSize", "250"));
      hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", config.getProperty("db.props.prepStmtCacheSqlLimit", "2048"));

      // Add more useful properties for better MySQL connection handling
      hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
      hikariConfig.addDataSourceProperty("useLocalSessionState", "true");
      hikariConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
      hikariConfig.addDataSourceProperty("cacheResultSetMetadata", "true");
      hikariConfig.addDataSourceProperty("cacheServerConfiguration", "true");
      hikariConfig.addDataSourceProperty("elideSetAutoCommits", "true");
      hikariConfig.addDataSourceProperty("maintainTimeStats", "false");

      // Enable connection testing
      hikariConfig.addDataSourceProperty("testWhileIdle", "true");
      hikariConfig.addDataSourceProperty("testOnBorrow", "true");
      hikariConfig.addDataSourceProperty("validationQuery", "SELECT 1");

      dataSource = new HikariDataSource(hikariConfig);

      // Test connection - using fully qualified type to avoid ambiguity
      try (java.sql.Connection conn = dataSource.getConnection()) {
        if (!conn.isValid(5)) {  // Removed unnecessary cast
          System.err.println("Database connection test failed. Please check your database configuration.");
        } else {
          System.out.println("Database connection established successfully.");
        }
      }
    } catch (Exception e) {
      System.err.println("Failed to initialize database connection pool: " + e.getMessage());
      e.printStackTrace();
      // Continue with a null dataSource - operations will fail but application won't crash immediately
    }
  }

  // Get database connection with proper error handling
  public static java.sql.Connection getConnection() throws SQLException {
    if (dataSource == null) {
      throw new SQLException("Database connection pool is not initialized.");
    }
    java.sql.Connection conn = dataSource.getConnection();
    if (conn == null) {
      throw new SQLException("Failed to get a connection from the pool.");
    }
    return conn;
  }

  // Database initialization with better error handling
  public static void initDatabase() throws SQLException {
    try (java.sql.Connection conn = getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // Create tasks table
        stmt.execute("CREATE TABLE IF NOT EXISTS tasks (" +
            "task_id VARCHAR(36) PRIMARY KEY, " +
            "total_lines INT, " +
            "processed_lines INT DEFAULT 0, " +
            "map_phase_complete BOOLEAN DEFAULT FALSE, " +
            "status VARCHAR(20) DEFAULT 'RUNNING', " +
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

        // Create results table
        stmt.execute("CREATE TABLE IF NOT EXISTS word_counts (" +
            "task_id VARCHAR(36), " +
            "word VARCHAR(100), " +
            "count INT, " +
            "PRIMARY KEY (task_id, word))");

        // Create Worker heartbeat table
        stmt.execute("CREATE TABLE IF NOT EXISTS worker_heartbeats (" +
            "worker_id VARCHAR(36) PRIMARY KEY, " +
            "worker_type VARCHAR(10), " +
            "last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
            "status VARCHAR(20) DEFAULT 'ACTIVE')");

        System.out.println("Database tables verified/created successfully.");
      }
    } catch (SQLException e) {
      System.err.println("Database initialization failed with error: " + e.getMessage());
      e.printStackTrace();
      throw e; // Rethrow to let the caller know initialization failed
    }
  }

  // Map phase processing
  public static class Mapper {
    public static void process(String line, String taskId, Channel channel) throws IOException {
      if (line == null || taskId == null || channel == null) {
        throw new IllegalArgumentException("Mapper process received null argument(s)");
      }

      // Local aggregation of counts for the same word
      Map<String, Integer> localCounts = new HashMap<>();

      // Split words
      String[] words = line.split("\\s+");
      for (String word : words) {
        word = word.toLowerCase().replaceAll("[^a-z]", "");
        if (!word.isEmpty()) {
          // Aggregate counts locally
          localCounts.merge(word, 1, Integer::sum);
        }
      }

      // Route to different Reducers based on word hash
      for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
        String word = entry.getKey();
        int count = entry.getValue();

        // Use consistent hashing to determine routing key
        int hash = Math.abs(word.hashCode() % REDUCE_SHARDS);
        String routingKey = REDUCE_QUEUE + "." + hash;

        try {
          channel.basicPublish("", routingKey, null,
              (taskId + ":" + word + ":" + count).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
          System.err.println("Failed to publish to queue " + routingKey + ": " + e.getMessage());
          throw e; // Rethrow to let caller handle the error
        }
      }
    }
  }

  // Reduce phase processing
  public static class Reducer {
    private Map<String, Map<String, Integer>> taskWordCounts = new ConcurrentHashMap<>();
    private Map<String, Integer> wordCounters = new ConcurrentHashMap<>(); // Track word count for each task

    public void process(String message) {
      if (message == null || message.isEmpty()) {
        System.err.println("Reducer received empty message. Skipping.");
        return;
      }

      String[] parts = message.split(":");
      if (parts.length == 3) {
        String taskId = parts[0];
        String word = parts[1];
        int count;

        try {
          count = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
          System.err.println("Invalid count format in message: " + message);
          return;
        }

        // Update counts in memory
        taskWordCounts
            .computeIfAbsent(taskId, k -> new ConcurrentHashMap<>())
            .merge(word, count, Integer::sum);

        // Update word counter
        int currentCount = wordCounters.getOrDefault(taskId, 0) + 1;
        wordCounters.put(taskId, currentCount);

        // If batch threshold reached, save to database
        if (currentCount >= WORD_COUNT_BATCH_SIZE) {
          try {
            saveResultsForTask(taskId);
            // Reset counter
            wordCounters.put(taskId, 0);
          } catch (SQLException e) {
            System.err.println("Error saving batch results for task " + taskId + ": " + e.getMessage());
            e.printStackTrace();
          }
        }
      } else {
        System.err.println("Malformed message format: " + message + " (expected 3 parts separated by ':')");
      }
    }

    // Save results for a specific task with improved error handling
    private void saveResultsForTask(String taskId) throws SQLException {
      Map<String, Integer> wordCounts = taskWordCounts.get(taskId);
      if (wordCounts == null || wordCounts.isEmpty()) {
        return;
      }

      // Create snapshot of current data to avoid affecting writes from other threads during clearing
      Map<String, Integer> snapshot = new HashMap<>(wordCounts);
      // Remove processed entries from the original map
      for (String key : snapshot.keySet()) {
        wordCounts.remove(key);
      }

      java.sql.Connection conn = null;
      try {
        conn = getConnection();
        conn.setAutoCommit(false);

        try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO word_counts (task_id, word, count) VALUES (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE count = count + VALUES(count)")) {

          int batchCount = 0;
          for (Map.Entry<String, Integer> entry : snapshot.entrySet()) {
            ps.setString(1, taskId);
            ps.setString(2, entry.getKey());
            ps.setInt(3, entry.getValue());
            ps.addBatch();
            batchCount++;

            // Execute batch every DB_BATCH_SIZE records
            if (batchCount >= DB_BATCH_SIZE) {
              ps.executeBatch();
              batchCount = 0;
            }
          }

          // Execute remaining batch
          if (batchCount > 0) {
            ps.executeBatch();
          }

          conn.commit();
        } catch (SQLException e) {
          if (conn != null) {
            try {
              conn.rollback();
            } catch (SQLException rollbackEx) {
              System.err.println("Error during rollback: " + rollbackEx.getMessage());
            }
          }
          throw e;
        }
      } finally {
        if (conn != null) {
          try {
            conn.setAutoCommit(true);
            conn.close();
          } catch (SQLException e) {
            System.err.println("Error closing connection: " + e.getMessage());
          }
        }
      }
    }

    // Save all results to database
    public void saveResultsToDatabase() {
      for (String taskId : new HashSet<>(taskWordCounts.keySet())) {
        try {
          saveResultsForTask(taskId);
          System.out.println(" [x] Saved results for task " + taskId + " to database");
        } catch (SQLException e) {
          System.err.println("Error saving results for task " + taskId + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }

    // Check if task has unsaved results
    public boolean hasUnprocessedData(String taskId) {
      Map<String, Integer> counts = taskWordCounts.get(taskId);
      return counts != null && !counts.isEmpty();
    }
  }

  // Update task progress (batch) with better error handling
  public static void updateProcessedCount(String taskId, int count) {
    if (count <= 0) {
      return; // No need to update for zero or negative counts
    }

    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "UPDATE tasks SET processed_lines = processed_lines + ? WHERE task_id = ?")) {
      ps.setInt(1, count);
      ps.setString(2, taskId);
      int updatedRows = ps.executeUpdate();

      if (updatedRows == 0) {
        System.err.println("Warning: Task " + taskId + " not found when updating progress");
      }
    } catch (SQLException e) {
      System.err.println("Error updating progress for task " + taskId + ": " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Check and mark Map phase complete
  public static boolean checkAndMarkMapPhaseComplete(String taskId) {
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "UPDATE tasks SET map_phase_complete = TRUE " +
                 "WHERE task_id = ? AND processed_lines >= total_lines AND map_phase_complete = FALSE")) {
      ps.setString(1, taskId);
      int updated = ps.executeUpdate();
      return updated > 0; // If records were updated, it means it's now marked as complete
    } catch (SQLException e) {
      System.err.println("Error marking map phase complete for task " + taskId + ": " + e.getMessage());
      e.printStackTrace();
      return false;
    }
  }

  // Get task information
  public static Map<String, Object> getTaskInfo(String taskId) {
    Map<String, Object> info = new HashMap<>();
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT total_lines, processed_lines, map_phase_complete, status FROM tasks WHERE task_id = ?")) {
      ps.setString(1, taskId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          info.put("totalLines", rs.getInt("total_lines"));
          info.put("processedLines", rs.getInt("processed_lines"));
          info.put("mapPhaseComplete", rs.getBoolean("map_phase_complete"));
          info.put("status", rs.getString("status"));
          System.out.println("Task " + taskId + " info retrieved successfully");
        } else {
          System.err.println("Warning: Task " + taskId + " not found in database");
        }
      }
    } catch (SQLException e) {
      System.err.println("Error getting info for task " + taskId + ": " + e.getMessage());
      e.printStackTrace();
    }
    return info;
  }

  // Update Worker heartbeat
  public static void updateWorkerHeartbeat(String workerId, String workerType) {
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "INSERT INTO worker_heartbeats (worker_id, worker_type, last_heartbeat) " +
                 "VALUES (?, ?, CURRENT_TIMESTAMP) " +
                 "ON DUPLICATE KEY UPDATE last_heartbeat = CURRENT_TIMESTAMP")) {
      ps.setString(1, workerId);
      ps.setString(2, workerType);
      ps.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error updating heartbeat for worker " + workerId + ": " + e.getMessage());
      // Don't throw exception for heartbeat failures
    }
  }

  // Start Map Worker with improved error handling
  public static void startMapWorker(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = null;
    Channel channel = null;

    try {
      // Establish connection
      rabbitConnection = factory.newConnection();
      channel = rabbitConnection.createChannel();

      // Declare queues with durable flag
      channel.queueDeclare(MAP_QUEUE, true, false, false, null);
      channel.queueDeclare(MAP_COMPLETE_QUEUE, true, false, false, null);

      // Declare multiple Reduce queue shards
      for (int i = 0; i < REDUCE_SHARDS; i++) {
        channel.queueDeclare(REDUCE_QUEUE + "." + i, true, false, false, null);
        System.out.println(" [*] Map Worker " + workerId + " declared queue " + REDUCE_QUEUE + "." + i);
      }


      System.out.println(" [*] Map Worker " + workerId + " waiting for messages");

      // Local counters for batch updating progress
      Map<String, Integer> localProgressCounters = new HashMap<>();

      // Start heartbeat thread
      int heartbeatInterval = Integer.parseInt(config.getProperty("worker.heartbeat.interval", "30"));
      ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
      heartbeatExecutor.scheduleAtFixedRate(() -> {
        updateWorkerHeartbeat(workerId, "MAP");
      }, 0, heartbeatInterval, TimeUnit.SECONDS);

      // Set QoS
      int prefetchCount = Integer.parseInt(config.getProperty("rabbit.prefetch", "10"));
      channel.basicQos(prefetchCount);

      // Create a final reference to channel for use in lambda
      final Channel finalChannel = channel;

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        AMQP.BasicProperties props = delivery.getProperties();

        // Get task ID from message properties
        Map<String, Object> headers = props.getHeaders();
        if (headers == null) {
          System.err.println("Message has no headers. Rejecting.");
          finalChannel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
          return;
        }

        Object headerValue = headers.get("task_id");
        String taskId;
        if (headerValue instanceof String) {
          taskId = (String) headerValue;
        } else if (headerValue instanceof com.rabbitmq.client.LongString) {
          taskId = ((com.rabbitmq.client.LongString) headerValue).toString();
        } else if (headerValue != null) {
          taskId = headerValue.toString();
        } else {
          System.err.println("Message has no task_id header. Rejecting.");
          finalChannel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
          return;
        }
        if (taskId == null) {
          System.err.println("Message has no task_id header. Rejecting.");
          finalChannel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
          return;
        }

        System.out.println(" [x] Map Worker received [Task: " + taskId + "]");

        try {
          // Process message
          Mapper.process(message, taskId, finalChannel);

          // Update local counter
          localProgressCounters.merge(taskId, 1, Integer::sum);

          // If batch threshold reached, update database
          int currentCount = localProgressCounters.get(taskId);
          if (currentCount >= PROGRESS_UPDATE_BATCH) {
            updateProcessedCount(taskId, currentCount);
            localProgressCounters.put(taskId, 0);

            // Check if task is complete
            Map<String, Object> taskInfo = getTaskInfo(taskId);
            if (taskInfo.isEmpty()) {
              System.err.println("Could not retrieve task info for " + taskId);
              finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
              return;
            }

            int processedLines = (int) taskInfo.get("processedLines");
            int totalLines = (int) taskInfo.get("totalLines");

            if (processedLines >= totalLines && checkAndMarkMapPhaseComplete(taskId)) {
              System.out.println(" [x] Map phase complete for task " + taskId);
              // Send Map phase complete signal
              finalChannel.basicPublish("", MAP_COMPLETE_QUEUE, null,
                  taskId.getBytes(StandardCharsets.UTF_8));
            }
          }

          finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
          System.err.println("Error processing map message: " + e.getMessage());
          e.printStackTrace();
          // Message processing failed, requeue
          finalChannel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        }
      };

      // Set up consumer with manual acknowledgment
      channel.basicConsume(MAP_QUEUE, false, deliverCallback, consumerTag -> { });

      // Add shutdown hook to ensure resources are released properly
      Channel finalChannel1 = channel;
      com.rabbitmq.client.Connection finalRabbitConnection = rabbitConnection;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          // Submit any unsaved progress
          for (Map.Entry<String, Integer> entry : localProgressCounters.entrySet()) {
            if (entry.getValue() > 0) {
              updateProcessedCount(entry.getKey(), entry.getValue());
            }
          }

          // Update worker status
          try (java.sql.Connection conn = getConnection();
               PreparedStatement ps = conn.prepareStatement(
                   "UPDATE worker_heartbeats SET status = 'INACTIVE' WHERE worker_id = ?")) {
            ps.setString(1, workerId);
            ps.executeUpdate();
          }

          heartbeatExecutor.shutdown();
          if (finalChannel1 != null && finalChannel1.isOpen()) {
            finalChannel1.close();
          }
          if (finalRabbitConnection != null && finalRabbitConnection.isOpen()) {
            finalRabbitConnection.close();
          }
        } catch (Exception e) {
          System.err.println("Error during shutdown: " + e.getMessage());
          e.printStackTrace();
        }
      }));

      // Keep main thread alive
      Thread.currentThread().join();

    } catch (Exception e) {
      System.err.println("Error starting Map Worker: " + e.getMessage());
      e.printStackTrace();

      // Clean up in case of error
      if (channel != null && channel.isOpen()) {
        try {
          channel.close();
        } catch (Exception closeEx) {
          System.err.println("Error closing channel: " + closeEx.getMessage());
        }
      }

      if (rabbitConnection != null && rabbitConnection.isOpen()) {
        try {
          rabbitConnection.close();
        } catch (Exception closeEx) {
          System.err.println("Error closing connection: " + closeEx.getMessage());
        }
      }

      throw e; // Rethrow to signal startup failure
    }
  }

  // Start Reduce Worker with improved error handling
  public static void startReduceWorker(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = null;
    Channel channel = null;
    ScheduledExecutorService heartbeatExecutor = null;
    ScheduledExecutorService saveExecutor = null;

    try {
      rabbitConnection = factory.newConnection();
      channel = rabbitConnection.createChannel();

      // Declare queues and exchanges
      channel.queueDeclare(MAP_COMPLETE_QUEUE, true, false, false, null);
      channel.queueDeclare(RESULT_QUEUE, true, false, false, null);

      // Select shards to process
      int shardId;
      String shardMode = config.getProperty("reduce.shard.mode", "random");

      if ("random".equals(shardMode)) {
        // Randomly select a shard
        shardId = new Random().nextInt(REDUCE_SHARDS);
      } else if ("all".equals(shardMode)) {
        // Process all shards (this is just an example, actual implementation might be more complex)
        shardId = -1;
      } else {
        // Read specified shard from configuration
        shardId = Integer.parseInt(config.getProperty("reduce.shard.id", "0"));
      }

      Reducer reducer = new Reducer();
      List<String> consumedQueues = new ArrayList<>();

      // If processing all shards
      if (shardId == -1) {
        System.out.println(" [*] Reduce Worker " + workerId + " waiting for messages on ALL shards");
        for (int i = 0; i < REDUCE_SHARDS; i++) {
          String queueName = REDUCE_QUEUE + "." + i;
          channel.queueDeclare(queueName, true, false, false, null);
          consumedQueues.add(queueName);
        }
      } else {
        // Process a single shard
        String reduceQueue = REDUCE_QUEUE + "." + shardId;
        channel.queueDeclare(reduceQueue, true, false, false, null);
        consumedQueues.add(reduceQueue);
        System.out.println(" [*] Reduce Worker " + workerId + " waiting for messages on shard " + shardId);
      }

      // Start heartbeat thread
      int heartbeatInterval = Integer.parseInt(config.getProperty("worker.heartbeat.interval", "30"));
      heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
      heartbeatExecutor.scheduleAtFixedRate(() -> {
        updateWorkerHeartbeat(workerId, "REDUCE");
      }, 0, heartbeatInterval, TimeUnit.SECONDS);

      // Periodically save results
      int saveInterval = Integer.parseInt(config.getProperty("reducer.save.interval", "30"));
      saveExecutor = Executors.newSingleThreadScheduledExecutor();
      saveExecutor.scheduleAtFixedRate(() -> {
        try {
          reducer.saveResultsToDatabase();
        } catch (Exception e) {
          System.err.println("Error in scheduled save: " + e.getMessage());
          e.printStackTrace();
        }
      }, saveInterval, saveInterval, TimeUnit.SECONDS);

      // Final reference to channel for lambda
      final Channel finalChannel = channel;

      // Process regular Reduce messages
      DeliverCallback reduceCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

        try {
          reducer.process(message);
          finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
          System.err.println("Error processing reduce message: " + e.getMessage());
          e.printStackTrace();
          finalChannel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        }
      };

      // Process Map complete signals
      DeliverCallback mapCompleteCallback = (consumerTag, delivery) -> {
        String taskId = new String(delivery.getBody(), StandardCharsets.UTF_8);
        System.out.println(" [x] Received map phase complete signal for task: " + taskId);

        // Ensure all results are saved
        try {
          if (reducer.hasUnprocessedData(taskId)) {
            reducer.saveResultsToDatabase();
          }

          // Wait a while to ensure all Reducers complete processing
          int completeDelay = Integer.parseInt(config.getProperty("reducer.complete.delay", "5000"));
          Thread.sleep(completeDelay);

          // Send task complete signal
          finalChannel.basicPublish("", RESULT_QUEUE, null,
              ("__COMPLETE__:" + taskId).getBytes(StandardCharsets.UTF_8));
          System.out.println(" [x] Sent completion signal for task " + taskId);

        } catch (Exception e) {
          System.err.println("Error processing map complete signal: " + e.getMessage());
          e.printStackTrace();
        } finally {
          try {
            finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          } catch (IOException e) {
            System.err.println("Error acknowledging map complete signal: " + e.getMessage());
          }
        }
      };

      // Set QoS
      int prefetchCount = Integer.parseInt(config.getProperty("rabbit.prefetch", "10"));
      channel.basicQos(prefetchCount);

      // Consume all required queues
      for (String queueName : consumedQueues) {
        channel.basicConsume(queueName, false, reduceCallback, consumerTag -> { });
      }
      channel.basicConsume(MAP_COMPLETE_QUEUE, false, mapCompleteCallback, consumerTag -> { });

      // Add shutdown hook
      ScheduledExecutorService finalHeartbeatExecutor = heartbeatExecutor;
      ScheduledExecutorService finalSaveExecutor = saveExecutor;
      Channel finalChannel1 = channel;
      com.rabbitmq.client.Connection finalRabbitConnection = rabbitConnection;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          // Save any unsaved results
          reducer.saveResultsToDatabase();

          // Update worker status
          try (java.sql.Connection conn = getConnection();
               PreparedStatement ps = conn.prepareStatement(
                   "UPDATE worker_heartbeats SET status = 'INACTIVE' WHERE worker_id = ?")) {
            ps.setString(1, workerId);
            ps.executeUpdate();
          }

          if (finalHeartbeatExecutor != null) finalHeartbeatExecutor.shutdown();
          if (finalSaveExecutor != null) finalSaveExecutor.shutdown();
          if (finalChannel1 != null && finalChannel1.isOpen()) finalChannel1.close();
          if (finalRabbitConnection != null && finalRabbitConnection.isOpen()) finalRabbitConnection.close();
        } catch (Exception e) {
          System.err.println("Error during shutdown: " + e.getMessage());
          e.printStackTrace();
        }
      }));

      // Keep the main thread alive
      Thread.currentThread().join();

    } catch (Exception e) {
      System.err.println("Error starting Reduce Worker: " + e.getMessage());
      e.printStackTrace();

      // Clean up resources
      if (heartbeatExecutor != null) heartbeatExecutor.shutdown();
      if (saveExecutor != null) saveExecutor.shutdown();
      if (channel != null && channel.isOpen()) {
        try {
          channel.close();
        } catch (Exception ex) {
          System.err.println("Error closing channel: " + ex.getMessage());
        }
      }
      if (rabbitConnection != null && rabbitConnection.isOpen()) {
        try {
          rabbitConnection.close();
        } catch (Exception ex) {
          System.err.println("Error closing connection: " + ex.getMessage());
        }
      }

      throw e; // Rethrow to signal startup failure
    }
  }

  // Start Result Collector with improved error handling
  public static void startResultCollector(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = null;
    Channel channel = null;
    ScheduledExecutorService heartbeatExecutor = null;

    try {
      rabbitConnection = factory.newConnection();
      channel = rabbitConnection.createChannel();

      channel.queueDeclare(RESULT_QUEUE, true, false, false, null);

      System.out.println(" [*] Result Collector " + workerId + " waiting for results");

      // Start heartbeat thread
      int heartbeatInterval = Integer.parseInt(config.getProperty("worker.heartbeat.interval", "30"));
      heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
      heartbeatExecutor.scheduleAtFixedRate(() -> {
        updateWorkerHeartbeat(workerId, "RESULT");
      }, 0, heartbeatInterval, TimeUnit.SECONDS);

      // Final reference to channel for lambda
      final Channel finalChannel = channel;

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

        try {
          // Check if it's a completion signal
          if (message.startsWith("__COMPLETE__:")) {
            String taskId = message.substring("__COMPLETE__:".length());
            System.out.println(" [x] Received completion signal for task: " + taskId);

            // Update task status to completed
            try (java.sql.Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                     "UPDATE tasks SET status = 'COMPLETED' WHERE task_id = ?")) {
              ps.setString(1, taskId);
              int rowsUpdated = ps.executeUpdate();

              if (rowsUpdated == 0) {
                System.err.println("Warning: Task " + taskId + " not found or already completed");
              } else {
                // Get and print final results
                printTaskResults(taskId);
              }
            } catch (SQLException e) {
              System.err.println("Error updating task status: " + e.getMessage());
              e.printStackTrace();
            }
          } else {
            System.err.println("Unknown message format received: " + message);
          }

          finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
          System.err.println("Error processing result message: " + e.getMessage());
          e.printStackTrace();
          finalChannel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        }
      };

      // Set QoS
      int prefetchCount = Integer.parseInt(config.getProperty("rabbit.prefetch", "10"));
      channel.basicQos(prefetchCount);

      channel.basicConsume(RESULT_QUEUE, false, deliverCallback, consumerTag -> { });

      // Add shutdown hook
      com.rabbitmq.client.Connection finalRabbitConnection = rabbitConnection;
      Channel finalChannel1 = channel;
      ScheduledExecutorService finalHeartbeatExecutor = heartbeatExecutor;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          // Update worker status
          try (java.sql.Connection conn = getConnection();
               PreparedStatement ps = conn.prepareStatement(
                   "UPDATE worker_heartbeats SET status = 'INACTIVE' WHERE worker_id = ?")) {
            ps.setString(1, workerId);
            ps.executeUpdate();
          }

          if (finalHeartbeatExecutor != null) finalHeartbeatExecutor.shutdown();
          if (finalChannel1 != null && finalChannel1.isOpen()) finalChannel1.close();
          if (finalRabbitConnection != null && finalRabbitConnection.isOpen()) finalRabbitConnection.close();
        } catch (Exception e) {
          System.err.println("Error during shutdown: " + e.getMessage());
          e.printStackTrace();
        }
      }));

      // Keep the main thread alive
      Thread.currentThread().join();

    } catch (Exception e) {
      System.err.println("Error starting Result Collector: " + e.getMessage());
      e.printStackTrace();

      // Clean up resources
      if (heartbeatExecutor != null) heartbeatExecutor.shutdown();
      if (channel != null && channel.isOpen()) {
        try {
          channel.close();
        } catch (Exception ex) {
          System.err.println("Error closing channel: " + ex.getMessage());
        }
      }
      if (rabbitConnection != null && rabbitConnection.isOpen()) {
        try {
          rabbitConnection.close();
        } catch (Exception ex) {
          System.err.println("Error closing connection: " + ex.getMessage());
        }
      }

      throw e;
    }
  }

  // Print task final results with better error handling
  private static void printTaskResults(String taskId) {
    int resultLimit = Integer.parseInt(config.getProperty("result.limit", "100"));
    String outputDir = config.getProperty("result.output.dir", ".");

    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT word, count FROM word_counts WHERE task_id = ? ORDER BY count DESC LIMIT ?")) {
      ps.setString(1, taskId);
      ps.setInt(2, resultLimit);

      System.out.println("\n=== Final Results for Task " + taskId + " ===");

      ResultSet rs = null;
      try {
        rs = ps.executeQuery();

        // Create output directory if it doesn't exist
        File dir = new File(outputDir);
        if (!dir.exists() && !dir.mkdirs()) {
          System.err.println("Warning: Could not create output directory: " + outputDir);
          System.out.println("Will write results to current directory instead.");
          outputDir = ".";
        }

        // Write results to file
        File outputFile = new File(outputDir, "wordcount_results_" + taskId + ".txt");
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
          int rank = 1;
          boolean hasResults = false;

          while (rs != null && rs.next()) {
            hasResults = true;
            String word = rs.getString("word");
            int count = rs.getInt("count");

            System.out.printf("%3d. %-20s: %d%n", rank++, word, count);
            writer.println(word + "\t" + count);
          }

          if (!hasResults) {
            System.out.println("No word count results found for task " + taskId);
            writer.println("No results found");
          }
        }

        System.out.println("=== Full results saved to " + outputDir + "/wordcount_results_" + taskId + ".txt ===\n");
      } finally {
        if (rs != null) {
          try {
            rs.close();
          } catch (SQLException e) {
            System.err.println("Error closing result set: " + e.getMessage());
          }
        }
      }
    } catch (SQLException | IOException e) {
      System.err.println("Error printing results for task " + taskId + ": " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Submit input file with improved error handling
  public static void submitInputFile(ConnectionFactory factory, String filePath) throws Exception {
    File inputFile = new File(filePath);
    if (!inputFile.exists() || !inputFile.canRead()) {
      throw new FileNotFoundException("Input file does not exist or cannot be read: " + filePath);
    }

    // Generate task ID
    String taskId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = null;
    Channel channel = null;

    try {
      // Calculate total lines
      int totalLines = 0;
      try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
        while (reader.readLine() != null) totalLines++;
      }

      if (totalLines == 0) {
        System.err.println("Warning: Input file is empty: " + filePath);
        return;
      }

      // Create task record in database
      try (java.sql.Connection conn = getConnection();
           PreparedStatement ps = conn.prepareStatement(
               "INSERT INTO tasks (task_id, total_lines) VALUES (?, ?)")) {
        ps.setString(1, taskId);
        ps.setInt(2, totalLines);
        ps.executeUpdate();
      }

      rabbitConnection = factory.newConnection();
      channel = rabbitConnection.createChannel();

      channel.queueDeclare(MAP_QUEUE, true, false, false, null);

      // Reset to beginning of file
      int linesProcessed = 0;
      int errorCount = 0;
      int maxErrors = Integer.parseInt(config.getProperty("submit.max.errors", "100"));

      try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
        String line;
        int batchSize = Integer.parseInt(config.getProperty("submit.batch.size", "1000"));

        while ((line = reader.readLine()) != null) {
          try {
            // Include task ID in message properties
            Map<String, Object> headers = new HashMap<>();
            headers.put("task_id", taskId);

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .headers(headers)
                .deliveryMode(2) // Persistent message
                .build();

            channel.basicPublish("", MAP_QUEUE, props, line.getBytes(StandardCharsets.UTF_8));
            linesProcessed++;

            // Output progress every batchSize lines
            if (linesProcessed % batchSize == 0) {
              System.out.println(" [x] Sent " + linesProcessed + "/" + totalLines + " lines for task " + taskId);
            }
          } catch (Exception e) {
            errorCount++;
            System.err.println("Error sending line " + (linesProcessed + 1) + ": " + e.getMessage());

            if (errorCount >= maxErrors) {
              System.err.println("Too many errors (" + errorCount + "). Stopping file submission.");
              break;
            }
          }
        }
      }

      if (linesProcessed > 0) {
        System.out.println(" [x] Input file submitted as task: " + taskId);
        System.out.println(" [x] Total lines: " + totalLines);
        System.out.println(" [x] Lines successfully sent: " + linesProcessed);

        if (errorCount > 0) {
          System.out.println(" [x] Lines with errors: " + errorCount);
        }
      } else {
        System.err.println("Failed to send any lines. Removing task record.");
        try (java.sql.Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM tasks WHERE task_id = ?")) {
          ps.setString(1, taskId);
          ps.executeUpdate();
        }
      }
    } finally {
      if (channel != null && channel.isOpen()) {
        try {
          channel.close();
        } catch (Exception e) {
          System.err.println("Error closing channel: " + e.getMessage());
        }
      }
      if (rabbitConnection != null && rabbitConnection.isOpen()) {
        try {
          rabbitConnection.close();
        } catch (Exception e) {
          System.err.println("Error closing connection: " + e.getMessage());
        }
      }
    }
  }

  // Monitor system status with improved error handling
  public static void startMonitor() {
    int monitorInterval = Integer.parseInt(config.getProperty("monitor.interval", "10"));
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    executor.scheduleAtFixedRate(() -> {
      try {
        java.sql.Connection conn = null;
        try {
          conn = getConnection();

          // Check Worker status
          try (PreparedStatement ps = conn.prepareStatement(
              "SELECT worker_type, COUNT(*) as count FROM worker_heartbeats " +
                  "WHERE last_heartbeat > DATE_SUB(NOW(), INTERVAL 2 MINUTE) AND status = 'ACTIVE' " +
                  "GROUP BY worker_type")) {
            System.out.println("\n=== Active Workers ===");
            try (ResultSet rs = ps.executeQuery()) {
              boolean hasWorkers = false;
              while (rs.next()) {
                hasWorkers = true;
                System.out.println(rs.getString("worker_type") + ": " + rs.getInt("count"));
              }
              if (!hasWorkers) {
                System.out.println("No active workers found!");
              }
            }
          }

          // Check running tasks
          try (PreparedStatement ps = conn.prepareStatement(
              "SELECT task_id, total_lines, processed_lines, status, " +
                  "ROUND((processed_lines / total_lines) * 100, 2) as progress " +
                  "FROM tasks WHERE status = 'RUNNING' ORDER BY created_at DESC LIMIT 5")) {
            System.out.println("\n=== Running Tasks ===");
            try (ResultSet rs = ps.executeQuery()) {
              boolean hasTasks = false;
              while (rs.next()) {
                hasTasks = true;
                String shortId = rs.getString("task_id");
                if (shortId.length() > 8) {
                  shortId = shortId.substring(0, 8) + "...";
                }

                System.out.printf("Task %s: %d/%d lines (%.2f%%) [%s]%n",
                    shortId,
                    rs.getInt("processed_lines"),
                    rs.getInt("total_lines"),
                    rs.getDouble("progress"),
                    rs.getString("status"));
              }
              if (!hasTasks) {
                System.out.println("No running tasks found!");
              }
            }
          }

          // Check recently completed tasks
          try (PreparedStatement ps = conn.prepareStatement(
              "SELECT task_id, total_lines, processed_lines, " +
                  "DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') as started " +
                  "FROM tasks WHERE status = 'COMPLETED' " +
                  "ORDER BY created_at DESC LIMIT 5")) {
            System.out.println("\n=== Recently Completed Tasks ===");
            try (ResultSet rs = ps.executeQuery()) {
              boolean hasTasks = false;
              while (rs.next()) {
                hasTasks = true;
                String shortId = rs.getString("task_id");
                if (shortId.length() > 8) {
                  shortId = shortId.substring(0, 8) + "...";
                }

                System.out.printf("Task %s: %d lines (started at %s)%n",
                    shortId,
                    rs.getInt("total_lines"),
                    rs.getString("started"));
              }
              if (!hasTasks) {
                System.out.println("No completed tasks found!");
              }
            }
          }

          System.out.println("\nUse Ctrl+C to exit monitor.");
        } finally {
          if (conn != null) {
            try {
              conn.close();
            } catch (SQLException e) {
              System.err.println("Error closing connection: " + e.getMessage());
            }
          }
        }
      } catch (SQLException e) {
        System.err.println("Error monitoring system: " + e.getMessage());
        e.printStackTrace();
      }
    }, 0, monitorInterval, TimeUnit.SECONDS);

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      executor.shutdown();
      System.out.println("Monitor stopped.");
    }));

    // Keep main thread running
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      executor.shutdown();
    }
  }
  public static void testQueue(ConnectionFactory factory) throws Exception {
    try (com.rabbitmq.client.Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {

      // Ensure queue exists
      channel.queueDeclare(MAP_QUEUE, true, false, false, null);

      // Send simple message without complex headers
      String message = "TEST_MESSAGE";
      channel.basicPublish("", MAP_QUEUE, null, message.getBytes(StandardCharsets.UTF_8));
      System.out.println(" [x] Sent test message: " + message);
    }
  }
  public static void main(String[] args) {
    // Allow configuration file path to be specified via command line arguments
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("--config") || args[i].equals("-c")) {
        System.setProperty("config.path", args[i + 1]);

        // Rearrange args, removing config options
        String[] newArgs = new String[args.length - 2];
        System.arraycopy(args, 0, newArgs, 0, i);
        if (i + 2 < args.length) {
          System.arraycopy(args, i + 2, newArgs, i, args.length - i - 2);
        }
        args = newArgs;
        break;
      }
    }

    if (args.length < 1) {
      System.err.println("Usage: java -jar wordcount.jar [--config <path>] [map|reduce|result|submit <file>|monitor]");
      System.exit(1);
    }

    // Initialize database
    try {
      initDatabase();
    } catch (SQLException e) {
      System.err.println("Database initialization failed: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    ConnectionFactory factory = new ConnectionFactory();
    try {
      // Read RabbitMQ connection info from configuration
      factory.setHost(config.getProperty("rabbit.host", "localhost"));
      if (config.containsKey("rabbit.port")) {
        factory.setPort(Integer.parseInt(config.getProperty("rabbit.port")));
      }
      if (config.containsKey("rabbit.username")) {
        factory.setUsername(config.getProperty("rabbit.username"));
      }
      if (config.containsKey("rabbit.password")) {
        factory.setPassword(config.getProperty("rabbit.password"));
      }
      if (config.containsKey("rabbit.virtualHost")) {
        factory.setVirtualHost(config.getProperty("rabbit.virtualHost"));
      }

      // Set connection recovery options
      factory.setAutomaticRecoveryEnabled(Boolean.parseBoolean(
          config.getProperty("rabbit.recovery.enabled", "true")));
      factory.setNetworkRecoveryInterval(Long.parseLong(
          config.getProperty("rabbit.recovery.interval", "5000")));

      // Set heartbeat interval
      factory.setRequestedHeartbeat(Integer.parseInt(
          config.getProperty("rabbit.heartbeat", "30")));

      String command = args[0];
      switch (command) {
        case "map":
          startMapWorker(factory);
          break;
        case "reduce":
          startReduceWorker(factory);
          break;
        case "result":
          startResultCollector(factory);
          break;
        case "submit":
          if (args.length < 2) {
            System.err.println("Please specify input file: submit <file>");
            System.exit(1);
          }
          submitInputFile(factory, args[1]);
          break;
        case "monitor":
          startMonitor();
          break;
        case "test":
          testQueue(factory);
          break;
        default:
          System.err.println("Unknown command: " + command);
          System.exit(1);
      }
    } catch (Exception e) {
      System.err.println("Application error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

  }
}