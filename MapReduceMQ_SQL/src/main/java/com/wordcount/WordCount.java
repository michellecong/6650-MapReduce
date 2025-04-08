package com.wordcount;

import com.rabbitmq.client.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
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
      System.out.println("Warning: Could not load configuration file: " + configPath);
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

    dataSource = new HikariDataSource(hikariConfig);
  }

  // Get database connection
  public static java.sql.Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  // Database initialization
  public static void initDatabase() throws SQLException {
    try (java.sql.Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
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
    }
  }

  // Map phase processing
  public static class Mapper {
    public static void process(String line, String taskId, Channel channel) throws IOException {
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

        channel.basicPublish("", routingKey, null,
            (taskId + ":" + word + ":" + count).getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  // Reduce phase processing
  public static class Reducer {
    private Map<String, Map<String, Integer>> taskWordCounts = new ConcurrentHashMap<>();
    private Map<String, Integer> wordCounters = new ConcurrentHashMap<>(); // Track word count for each task

    public void process(String message) {
      String[] parts = message.split(":");
      if (parts.length == 3) {
        String taskId = parts[0];
        String word = parts[1];
        int count = Integer.parseInt(parts[2]);

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
            System.err.println("Error saving batch results: " + e.getMessage());
          }
        }
      }
    }

    // Save results for a specific task
    private void saveResultsForTask(String taskId) throws SQLException {
      Map<String, Integer> wordCounts = taskWordCounts.get(taskId);
      if (wordCounts == null || wordCounts.isEmpty()) {
        return;
      }

      // Create snapshot of current data to avoid affecting writes from other threads during clearing
      Map<String, Integer> snapshot = new HashMap<>(wordCounts);
      wordCounts.clear();

      try (java.sql.Connection conn = getConnection()) {
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
          conn.rollback();
          throw e;
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
        }
      }
    }

    // Check if task has unsaved results
    public boolean hasUnprocessedData(String taskId) {
      Map<String, Integer> counts = taskWordCounts.get(taskId);
      return counts != null && !counts.isEmpty();
    }
  }

  // Update task progress (batch)
  public static void updateProcessedCount(String taskId, int count) {
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "UPDATE tasks SET processed_lines = processed_lines + ? WHERE task_id = ?")) {
      ps.setInt(1, count);
      ps.setString(2, taskId);
      ps.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error updating progress: " + e.getMessage());
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
      System.err.println("Error marking map phase complete: " + e.getMessage());
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
        }
      }
    } catch (SQLException e) {
      System.err.println("Error getting task info: " + e.getMessage());
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
      System.err.println("Error updating heartbeat: " + e.getMessage());
    }
  }

  // Start Map Worker
  public static void startMapWorker(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

    // Declare queues
    channel.queueDeclare(MAP_QUEUE, true, false, false, null);
    channel.queueDeclare(MAP_COMPLETE_QUEUE, true, false, false, null);

    // Declare multiple Reduce queue shards
    for (int i = 0; i < REDUCE_SHARDS; i++) {
      channel.queueDeclare(REDUCE_QUEUE + "." + i, true, false, false, null);
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

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      AMQP.BasicProperties props = delivery.getProperties();

      // Get task ID from message properties
      String taskId = (String) props.getHeaders().get("task_id");
      System.out.println(" [x] Map Worker received [Task: " + taskId + "]");

      try {
        // Process message
        Mapper.process(message, taskId, channel);

        // Update local counter
        localProgressCounters.merge(taskId, 1, Integer::sum);

        // If batch threshold reached, update database
        int currentCount = localProgressCounters.get(taskId);
        if (currentCount >= PROGRESS_UPDATE_BATCH) {
          updateProcessedCount(taskId, currentCount);
          localProgressCounters.put(taskId, 0);

          // Check if task is complete
          Map<String, Object> taskInfo = getTaskInfo(taskId);
          int processedLines = (int) taskInfo.get("processedLines") + currentCount;
          int totalLines = (int) taskInfo.get("totalLines");

          if (processedLines >= totalLines && checkAndMarkMapPhaseComplete(taskId)) {
            System.out.println(" [x] Map phase complete for task " + taskId);
            // Send Map phase complete signal
            channel.basicPublish("", MAP_COMPLETE_QUEUE, null,
                taskId.getBytes(StandardCharsets.UTF_8));
          }
        }

        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (Exception e) {
        System.err.println("Error processing map message: " + e.getMessage());
        // Message processing failed, requeue
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
      }
    };

    channel.basicConsume(MAP_QUEUE, false, deliverCallback, consumerTag -> { });

    // Add shutdown hook to ensure resources are released properly
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
        channel.close();
        rabbitConnection.close();
      } catch (Exception e) {
        System.err.println("Error during shutdown: " + e.getMessage());
      }
    }));
  }

  // Start Reduce Worker
  public static void startReduceWorker(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

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
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    heartbeatExecutor.scheduleAtFixedRate(() -> {
      updateWorkerHeartbeat(workerId, "REDUCE");
    }, 0, heartbeatInterval, TimeUnit.SECONDS);

    // Periodically save results
    int saveInterval = Integer.parseInt(config.getProperty("reducer.save.interval", "30"));
    ScheduledExecutorService saveExecutor = Executors.newSingleThreadScheduledExecutor();
    saveExecutor.scheduleAtFixedRate(() -> {
      try {
        reducer.saveResultsToDatabase();
      } catch (Exception e) {
        System.err.println("Error in scheduled save: " + e.getMessage());
      }
    }, saveInterval, saveInterval, TimeUnit.SECONDS);

    // Process regular Reduce messages
    DeliverCallback reduceCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

      try {
        reducer.process(message);
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (Exception e) {
        System.err.println("Error processing reduce message: " + e.getMessage());
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
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
        channel.basicPublish("", RESULT_QUEUE, null,
            ("__COMPLETE__:" + taskId).getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent completion signal for task " + taskId);

      } catch (Exception e) {
        System.err.println("Error processing map complete signal: " + e.getMessage());
      } finally {
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    };

    // Consume all required queues
    for (String queueName : consumedQueues) {
      channel.basicConsume(queueName, false, reduceCallback, consumerTag -> { });
    }
    channel.basicConsume(MAP_COMPLETE_QUEUE, false, mapCompleteCallback, consumerTag -> { });

    // Add shutdown hook
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

        heartbeatExecutor.shutdown();
        saveExecutor.shutdown();
        channel.close();
        rabbitConnection.close();
      } catch (Exception e) {
        System.err.println("Error during shutdown: " + e.getMessage());
      }
    }));
  }

  // Result collector
  public static void startResultCollector(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

    channel.queueDeclare(RESULT_QUEUE, true, false, false, null);

    System.out.println(" [*] Result Collector " + workerId + " waiting for results");

    // Start heartbeat thread
    int heartbeatInterval = Integer.parseInt(config.getProperty("worker.heartbeat.interval", "30"));
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    heartbeatExecutor.scheduleAtFixedRate(() -> {
      updateWorkerHeartbeat(workerId, "RESULT");
    }, 0, heartbeatInterval, TimeUnit.SECONDS);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

      // Check if it's a completion signal
      if (message.startsWith("__COMPLETE__:")) {
        String taskId = message.substring("__COMPLETE__:".length());
        System.out.println(" [x] Received completion signal for task: " + taskId);

        // Update task status to completed
        try (java.sql.Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE tasks SET status = 'COMPLETED' WHERE task_id = ?")) {
          ps.setString(1, taskId);
          ps.executeUpdate();
        } catch (SQLException e) {
          System.err.println("Error updating task status: " + e.getMessage());
        }

        // Get and print final results
        printTaskResults(taskId);
      }

      channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    };

    channel.basicConsume(RESULT_QUEUE, false, deliverCallback, consumerTag -> { });

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        // Update worker status
        try (java.sql.Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE worker_heartbeats SET status = 'INACTIVE' WHERE worker_id = ?")) {
          ps.setString(1, workerId);
          ps.executeUpdate();
        }

        heartbeatExecutor.shutdown();
        channel.close();
        rabbitConnection.close();
      } catch (Exception e) {
        System.err.println("Error during shutdown: " + e.getMessage());
      }
    }));
  }

  // Print task final results
  private static void printTaskResults(String taskId) {
    int resultLimit = Integer.parseInt(config.getProperty("result.limit", "100"));
    String outputDir = config.getProperty("result.output.dir", ".");
    
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT word, count FROM word_counts WHERE task_id = ? ORDER BY count DESC LIMIT ?")) {
      ps.setString(1, taskId);
      ps.setInt(2, resultLimit);

      System.out.println("\n=== Final Results for Task " + taskId + " ===");
      try (ResultSet rs = ps.executeQuery()) {
        // Write results to file
        File outputFile = new File(outputDir, "wordcount_results_" + taskId + ".txt");
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
          int rank = 1;
          while (rs.next()) {
            String word = rs.getString("word");
            int count = rs.getInt("count");

            System.out.printf("%3d. %-20s: %d%n", rank++, word, count);
            writer.println(word + "\t" + count);
          }
        }
      }
      System.out.println("=== Full results saved to " + outputDir + "/wordcount_results_" + taskId + ".txt ===\n");
    } catch (SQLException | IOException e) {
      System.err.println("Error printing results: " + e.getMessage());
    }
  }

  // Submit input file
  public static void submitInputFile(ConnectionFactory factory, String filePath) throws Exception {
    // Generate task ID
    String taskId = UUID.randomUUID().toString();

    // Calculate total lines
    int totalLines = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      while (reader.readLine() != null) totalLines++;
    }

    // Create task record in database
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "INSERT INTO tasks (task_id, total_lines) VALUES (?, ?)")) {
      ps.setString(1, taskId);
      ps.setInt(2, totalLines);
      ps.executeUpdate();
    }

    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

    channel.queueDeclare(MAP_QUEUE, true, false, false, null);

    // Reset to beginning of file
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      int lineCount = 0;
      int batchSize = Integer.parseInt(config.getProperty("submit.batch.size", "1000"));

      while ((line = reader.readLine()) != null) {
        // Include task ID in message properties
        Map<String, Object> headers = new HashMap<>();
        headers.put("task_id", taskId);

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
            .headers(headers)
            .deliveryMode(2) // Persistent message
            .build();

        channel.basicPublish("", MAP_QUEUE, props, line.getBytes(StandardCharsets.UTF_8));
        lineCount++;

        // Output progress every batchSize lines
        if (lineCount % batchSize == 0) {
          System.out.println(" [x] Sent " + lineCount + "/" + totalLines + " lines for task " + taskId);
        }
      }
    }

    System.out.println(" [x] Input file submitted as task: " + taskId);
    System.out.println(" [x] Total lines: " + totalLines);
    rabbitConnection.close();
  }

  // Monitor system status
  public static void startMonitor() {
    int monitorInterval = Integer.parseInt(config.getProperty("monitor.interval", "10"));
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(() -> {
      try (java.sql.Connection conn = getConnection()) {
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
              System.out.printf("Task %s: %d/%d lines (%.2f%%) [%s]%n",
                  rs.getString("task_id").substring(0, 8) + "...",
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
              System.out.printf("Task %s: %d lines (started at %s)%n",
                  rs.getString("task_id").substring(0, 8) + "...",
                  rs.getInt("total_lines"),
                  rs.getString("started"));
            }
            if (!hasTasks) {
              System.out.println("No completed tasks found!");
            }
          }
        }

        System.out.println("\nUse Ctrl+C to exit monitor.");
      } catch (SQLException e) {
        System.err.println("Error monitoring system: " + e.getMessage());
      }
    }, 0, monitorInterval, TimeUnit.SECONDS);

    // Keep main thread running
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      executor.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
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
      System.err.println("Usage: java WordCount [--config <path>] [map|reduce|result|submit <file>|monitor]");
      System.exit(1);
    }

    // Initialize database
    try {
      initDatabase();
    } catch (SQLException e) {
      System.err.println("Database initialization failed: " + e.getMessage());
      System.exit(1);
    }

    ConnectionFactory factory = new ConnectionFactory();
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
      default:
        System.err.println("Unknown command: " + command);
        System.exit(1);
    }
  }
}