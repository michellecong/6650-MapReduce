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
  private static final String MAP_QUEUE = "map_queue";
  private static final String REDUCE_QUEUE = "reduce_queue";
  private static final String RESULT_QUEUE = "result_queue";
  private static final String MAP_COMPLETE_QUEUE = "map_complete_queue";
  private static final String DB_URL = "jdbc:mysql://localhost:3306/wordcount";
  private static final String DB_USER = "user";
  private static final String DB_PASSWORD = "password";

  // constants for batch processing
  private static final int PROGRESS_UPDATE_BATCH = 100; // update progess every 100 lines
  private static final int WORD_COUNT_BATCH_SIZE = 1000; // batch size for word count
  private static final int DB_BATCH_SIZE = 500; // batch size for DB operations

  // db connection pool
  private static HikariDataSource dataSource;

  // initialize db connection pool
  static {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(DB_URL);
    config.setUsername(DB_USER);
    config.setPassword(DB_PASSWORD);
    config.setMaximumPoolSize(10);
    config.setMinimumIdle(5);
    config.setIdleTimeout(30000);
    config.setMaxLifetime(1800000);
    config.setConnectionTimeout(30000);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

    dataSource = new HikariDataSource(config);
  }

  // get a connection from the pool
  public static java.sql.Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  // initialize database
  public static void initDatabase() throws SQLException {
    try (java.sql.Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      // set up the database schema
      stmt.execute("CREATE TABLE IF NOT EXISTS tasks (" +
          "task_id VARCHAR(36) PRIMARY KEY, " +
          "total_lines INT, " +
          "processed_lines INT DEFAULT 0, " +
          "map_phase_complete BOOLEAN DEFAULT FALSE, " + // mark map phase complete
          "status VARCHAR(20) DEFAULT 'RUNNING', " +
          "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

      // create word count table
      stmt.execute("CREATE TABLE IF NOT EXISTS word_counts (" +
          "task_id VARCHAR(36), " +
          "word VARCHAR(100), " +
          "count INT, " +
          "PRIMARY KEY (task_id, word))");

      // create worker heartbeats table
      stmt.execute("CREATE TABLE IF NOT EXISTS worker_heartbeats (" +
          "worker_id VARCHAR(36) PRIMARY KEY, " +
          "worker_type VARCHAR(10), " +
          "last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
          "status VARCHAR(20) DEFAULT 'ACTIVE')");
    }
  }

  // map stage processor
  public static class Mapper {
    public static void process(String line, String taskId, Channel channel) throws IOException {
      // agregate word counts locally
      Map<String, Integer> localCounts = new HashMap<>();

      // split line into words
      String[] words = line.split("\\s+");
      for (String word : words) {
        word = word.toLowerCase().replaceAll("[^a-z]", "");
        if (!word.isEmpty()) {
          // aggregate word counts
          localCounts.merge(word, 1, Integer::sum);
        }
      }

      // publish aggregated counts to reduce queue based on hash
      for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
        String word = entry.getKey();
        int count = entry.getValue();

        // use hash to determine routing key
        int hash = Math.abs(word.hashCode() % 10); // 10 shards predefined
        String routingKey = REDUCE_QUEUE + "." + hash;

        channel.basicPublish("", routingKey, null,
            (taskId + ":" + word + ":" + count).getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  // Reduce stage processor
  public static class Reducer {
    private Map<String, Map<String, Integer>> taskWordCounts = new ConcurrentHashMap<>();
    private Map<String, Integer> wordCounters = new ConcurrentHashMap<>(); // record word counts

    public void process(String message) {
      String[] parts = message.split(":");
      if (parts.length == 3) {
        String taskId = parts[0];
        String word = parts[1];
        int count = Integer.parseInt(parts[2]);

        // update word counts for the specific task
        taskWordCounts
            .computeIfAbsent(taskId, k -> new ConcurrentHashMap<>())
            .merge(word, count, Integer::sum);

        // update local word counter
        int currentCount = wordCounters.getOrDefault(taskId, 0) + 1;
        wordCounters.put(taskId, currentCount);

        // if the count reaches the batch size, save to DB
        if (currentCount >= WORD_COUNT_BATCH_SIZE) {
          try {
            saveResultsForTask(taskId);
            // reset the local counter
            wordCounters.put(taskId, 0);
          } catch (SQLException e) {
            System.err.println("Error saving batch results: " + e.getMessage());
          }
        }
      }
    }

    // save results for a specific task to the database
    private void saveResultsForTask(String taskId) throws SQLException {
      Map<String, Integer> wordCounts = taskWordCounts.get(taskId);
      if (wordCounts == null || wordCounts.isEmpty()) {
        return;
      }

      try (java.sql.Connection conn = getConnection()) {
        conn.setAutoCommit(false);

        try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO word_counts (task_id, word, count) VALUES (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE count = count + VALUES(count)")) {

          int batchCount = 0;
          for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            ps.setString(1, taskId);
            ps.setString(2, entry.getKey());
            ps.setInt(3, entry.getValue());
            ps.addBatch();
            batchCount++;

            // execute batch if it reaches the threshold
            if (batchCount >= DB_BATCH_SIZE) {
              ps.executeBatch();
              batchCount = 0;
            }
          }

          // execute any remaining batch
          if (batchCount > 0) {
            ps.executeBatch();
          }

          conn.commit();
          // clear the local word counts for this task
          wordCounts.clear();
        } catch (SQLException e) {
          conn.rollback();
          throw e;
        }
      }
    }

    // save results to database (periodically)
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

    // check if there are unprocessed data for a specific task
    public boolean hasUnprocessedData(String taskId) {
      Map<String, Integer> counts = taskWordCounts.get(taskId);
      return counts != null && !counts.isEmpty();
    }
  }

  // update processed count in the database
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

  // check and mark map phase as complete
  public static boolean checkAndMarkMapPhaseComplete(String taskId) {
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "UPDATE tasks SET map_phase_complete = TRUE " +
                 "WHERE task_id = ? AND processed_lines >= total_lines AND map_phase_complete = FALSE")) {
      ps.setString(1, taskId);
      int updated = ps.executeUpdate();
      return updated > 0; // if updated, return true
    } catch (SQLException e) {
      System.err.println("Error marking map phase complete: " + e.getMessage());
      return false;
    }
  }

  // get task info
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

  // update worker heartbeat
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

  // start Map Worker
  public static void startMapWorker(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

    // declare queues and exchanges
    channel.queueDeclare(MAP_QUEUE, true, false, false, null);
    channel.queueDeclare(MAP_COMPLETE_QUEUE, true, false, false, null);

    // declare reduce queues
    for (int i = 0; i < 10; i++) {
      channel.queueDeclare(REDUCE_QUEUE + "." + i, true, false, false, null);
    }

    System.out.println(" [*] Map Worker " + workerId + " waiting for messages");

    // local progress counters
    Map<String, Integer> localProgressCounters = new HashMap<>();

    // start heartbeat thread
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    heartbeatExecutor.scheduleAtFixedRate(() -> {
      updateWorkerHeartbeat(workerId, "MAP");
    }, 0, 30, TimeUnit.SECONDS);

    channel.basicQos(10); // process 10 messages at a time

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      AMQP.BasicProperties props = delivery.getProperties();

      // get task ID from message properties
      String taskId = (String) props.getHeaders().get("task_id");
      System.out.println(" [x] Map Worker received [Task: " + taskId + "]");

      try {
        // process the message
        Mapper.process(message, taskId, channel);

        // update local progress counter
        localProgressCounters.merge(taskId, 1, Integer::sum);

        // check if we need to update the processed count
        int currentCount = localProgressCounters.get(taskId);
        if (currentCount >= PROGRESS_UPDATE_BATCH) {
          updateProcessedCount(taskId, currentCount);
          localProgressCounters.put(taskId, 0);

          // check if map phase is complete
          Map<String, Object> taskInfo = getTaskInfo(taskId);
          int processedLines = (int) taskInfo.get("processedLines") + currentCount;
          int totalLines = (int) taskInfo.get("totalLines");

          if (processedLines >= totalLines && checkAndMarkMapPhaseComplete(taskId)) {
            System.out.println(" [x] Map phase complete for task " + taskId);
            // send map complete signal
            channel.basicPublish("", MAP_COMPLETE_QUEUE, null,
                taskId.getBytes(StandardCharsets.UTF_8));
          }
        }

        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (Exception e) {
        System.err.println("Error processing map message: " + e.getMessage());
        // requeue the message
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
      }
    };

    channel.basicConsume(MAP_QUEUE, false, deliverCallback, consumerTag -> { });

    // add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        // submit any remaining progress
        for (Map.Entry<String, Integer> entry : localProgressCounters.entrySet()) {
          if (entry.getValue() > 0) {
            updateProcessedCount(entry.getKey(), entry.getValue());
          }
        }

        // 更新worker状态
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

  // start Reduce Worker
  public static void startReduceWorker(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

    // declare queues
    channel.queueDeclare(MAP_COMPLETE_QUEUE, true, false, false, null);
    channel.queueDeclare(RESULT_QUEUE, true, false, false, null);

    // declare reduce queues
    int shardId = new Random().nextInt(10);
    String reduceQueue = REDUCE_QUEUE + "." + shardId;
    channel.queueDeclare(reduceQueue, true, false, false, null);

    System.out.println(" [*] Reduce Worker " + workerId + " waiting for messages on shard " + shardId);

    Reducer reducer = new Reducer();

    // start heartbeat thread
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    heartbeatExecutor.scheduleAtFixedRate(() -> {
      updateWorkerHeartbeat(workerId, "REDUCE");
    }, 0, 30, TimeUnit.SECONDS);

    // save results to database every 30 seconds
    ScheduledExecutorService saveExecutor = Executors.newSingleThreadScheduledExecutor();
    saveExecutor.scheduleAtFixedRate(() -> {
      try {
        reducer.saveResultsToDatabase();
      } catch (Exception e) {
        System.err.println("Error in scheduled save: " + e.getMessage());
      }
    }, 30, 30, TimeUnit.SECONDS);  // inreased to 30 seconds

    // handle reduce messages
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

    // handle map complete messages
    DeliverCallback mapCompleteCallback = (consumerTag, delivery) -> {
      String taskId = new String(delivery.getBody(), StandardCharsets.UTF_8);
      System.out.println(" [x] Received map phase complete signal for task: " + taskId);

      // check if the map phase is complete
      try {
        if (reducer.hasUnprocessedData(taskId)) {
          reducer.saveResultsToDatabase();
        }

        // simulate some processing time
        Thread.sleep(5000);

        // send completion signal to result queue
        channel.basicPublish("", RESULT_QUEUE, null,
            ("__COMPLETE__:" + taskId).getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent completion signal for task " + taskId);

      } catch (Exception e) {
        System.err.println("Error processing map complete signal: " + e.getMessage());
      } finally {
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    };

    // consume messages
    channel.basicConsume(reduceQueue, false, reduceCallback, consumerTag -> { });
    channel.basicConsume(MAP_COMPLETE_QUEUE, false, mapCompleteCallback, consumerTag -> { });

    // add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        // submit any remaining results
        reducer.saveResultsToDatabase();

    // update worker status
          try (java.sql.Connection conn = getConnection();  // Fully qualify Connection with java.sql
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

  // start Result Collector
  public static void startResultCollector(ConnectionFactory factory) throws Exception {
    String workerId = UUID.randomUUID().toString();
    com.rabbitmq.client.Connection rabbitConnection = factory.newConnection();
    Channel channel = rabbitConnection.createChannel();

    channel.queueDeclare(RESULT_QUEUE, true, false, false, null);

    System.out.println(" [*] Result Collector " + workerId + " waiting for results");

    // start heartbeat thread
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    heartbeatExecutor.scheduleAtFixedRate(() -> {
      updateWorkerHeartbeat(workerId, "RESULT");
    }, 0, 30, TimeUnit.SECONDS);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

      // check if the message is a completion signal
      if (message.startsWith("__COMPLETE__:")) {
        String taskId = message.substring("__COMPLETE__:".length());
        System.out.println(" [x] Received completion signal for task: " + taskId);

        // update task status in the database
        try (java.sql.Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE tasks SET status = 'COMPLETED' WHERE task_id = ?")) {
          ps.setString(1, taskId);
          ps.executeUpdate();
        } catch (SQLException e) {
          System.err.println("Error updating task status: " + e.getMessage());
        }

        // print task results
        printTaskResults(taskId);
      }

      channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    };

    channel.basicConsume(RESULT_QUEUE, false, deliverCallback, consumerTag -> { });

//   add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        // submit any remaining results
        try (java.sql.Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE worker_heartbeats SET status = 'INACTIVE' WHERE worker_id = ?")) {
          ps.setString(1, workerId);
          ps.executeUpdate();
        }

        heartbeatExecutor.shutdown();
        channel.close();
        rabbitConnection.close();  // Changed from connection to rabbitConnection
      } catch (Exception e) {
        System.err.println("Error during shutdown: " + e.getMessage());
      }
    }));
  }

  // print task results
  private static void printTaskResults(String taskId) {
    try (java.sql.Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT word, count FROM word_counts WHERE task_id = ? ORDER BY count DESC LIMIT 100")) {
      ps.setString(1, taskId);

      System.out.println("\n=== Final Results for Task " + taskId + " ===");
      try (ResultSet rs = ps.executeQuery()) {
        // print top 100 results
        try (PrintWriter writer = new PrintWriter(new FileWriter("wordcount_results_" + taskId + ".txt"))) {
          int rank = 1;
          while (rs.next()) {
            String word = rs.getString("word");
            int count = rs.getInt("count");

            System.out.printf("%3d. %-20s: %d%n", rank++, word, count);
            writer.println(word + "\t" + count);
          }
        }
      }
      System.out.println("=== Full results saved to wordcount_results_" + taskId + ".txt ===\n");
    } catch (SQLException | IOException e) {
      System.err.println("Error printing results: " + e.getMessage());
    }
  }

  // submit input file
  public static void submitInputFile(ConnectionFactory factory, String filePath) throws Exception {
    // generate a unique task ID
    String taskId = UUID.randomUUID().toString();

    // count total lines in the input file
    int totalLines = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      while (reader.readLine() != null) totalLines++;
    }

    // insert task info into the database
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

    // read the input file and publish each line to the map queue
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      int lineCount = 0;

      while ((line = reader.readLine()) != null) {
        // publish each line to the map queue
        Map<String, Object> headers = new HashMap<>();
        headers.put("task_id", taskId);

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
            .headers(headers)
            .deliveryMode(2) 
            .build();

        channel.basicPublish("", MAP_QUEUE, props, line.getBytes(StandardCharsets.UTF_8));
        lineCount++;

        // update progress in the database
        if (lineCount % 1000 == 0) {
          System.out.println(" [x] Sent " + lineCount + "/" + totalLines + " lines for task " + taskId);
        }
      }
    }

    System.out.println(" [x] Input file submitted as task: " + taskId);
    System.out.println(" [x] Total lines: " + totalLines);
    rabbitConnection.close();
  }

  // monitor system status
  public static void startMonitor() {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(() -> {
      try (java.sql.Connection conn = getConnection()) {
        // check active workers
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

        // check running tasks
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

        // check recently completed tasks
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
    }, 0, 10, TimeUnit.SECONDS);

    // keep the main thread alive
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      executor.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: java WordCount [map|reduce|result|submit <file>|monitor]");
      System.exit(1);
    }

    // initialize the database
    try {
      initDatabase();
    } catch (SQLException e) {
      System.err.println("Database initialization failed: " + e.getMessage());
      System.exit(1);
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    // set RabbitMQ connection parameters
    factory.setAutomaticRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(5000); // 5 seconds to recover from network issues

    // set heartbeat parameters
    factory.setRequestedHeartbeat(30); // 30 seconds heartbeat interval

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