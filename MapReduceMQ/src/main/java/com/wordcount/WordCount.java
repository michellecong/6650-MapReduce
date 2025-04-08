package com.wordcount;



import com.rabbitmq.client.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class WordCount {
  private static final String MAP_QUEUE = "map_queue";
  private static final String REDUCE_QUEUE = "reduce_queue";
  private static final String RESULT_QUEUE = "result_queue";

  // Map阶段处理
  public static class Mapper {
    public static void process(String line, Channel channel) throws IOException {

      // 预处理：替换所有标点和特殊字符为空格
      line = line.replaceAll("[\\p{Punct}&&[^-]]", " ");
      // 先替换连字符为空格
      line = line.replaceAll("-", " ");

      // 拆分单词并发送到Reduce队列
      String[] words = line.split("\\s+");
      for (String word : words) {
        // 只保留字母，转为小写
        word = word.toLowerCase().replaceAll("[^a-z]", "");
        if (!word.isEmpty()) {
          // 使用单词作为路由键，将"1"作为计数发送
          channel.basicPublish("", REDUCE_QUEUE, null,
              (word + ":1").getBytes(StandardCharsets.UTF_8));
        }
      }
    }
  }

  // Reduce阶段处理
  public static class Reducer {
    private Map<String, Integer> wordCounts = new ConcurrentHashMap<>();

    public void process(String message) {
      String[] parts = message.split(":");
      if (parts.length == 2) {
        String word = parts[0];
        int count = Integer.parseInt(parts[1]);
        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
      }
    }

    public Map<String, Integer> getResults() {
      return wordCounts;
    }
  }

  // 启动Map Worker
  public static void startMapWorker(ConnectionFactory factory) throws Exception {
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(MAP_QUEUE, false, false, false, null);
    channel.queueDeclare(REDUCE_QUEUE, false, false, false, null);

    System.out.println(" [*] Map Worker waiting for messages");

    channel.basicQos(1); // 一次只处理一个消息

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      System.out.println(" [x] Map Worker received: " + message);

      try {
        Mapper.process(message, channel);
      } finally {
        System.out.println(" [x] Map Worker done");
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    };

    channel.basicConsume(MAP_QUEUE, false, deliverCallback, consumerTag -> { });
  }

  // 启动Reduce Worker
  public static void startReduceWorker(ConnectionFactory factory) throws Exception {
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(REDUCE_QUEUE, false, false, false, null);
    channel.queueDeclare(RESULT_QUEUE, false, false, false, null);

    System.out.println(" [*] Reduce Worker waiting for messages");

    Reducer reducer = new Reducer();

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

      try {
        reducer.process(message);
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (Exception e) {
        System.err.println("Error processing reduce message: " + e.getMessage());
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
      }
    };

    channel.basicConsume(REDUCE_QUEUE, false, deliverCallback, consumerTag -> { });

    // 定期发送结果到结果队列
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(() -> {
      try {
        Map<String, Integer> results = reducer.getResults();
        if (!results.isEmpty()) {
          for (Map.Entry<String, Integer> entry : results.entrySet()) {
            String resultMessage = entry.getKey() + ":" + entry.getValue();
            channel.basicPublish("", RESULT_QUEUE, null,
                resultMessage.getBytes(StandardCharsets.UTF_8));
          }
          System.out.println(" [x] Sent results: " + results.size() + " words");
        }
      } catch (Exception e) {
        System.err.println("Error sending results: " + e.getMessage());
      }
    }, 10, 10, TimeUnit.SECONDS);
  }

  // 结果收集器
  public static void startResultCollector(ConnectionFactory factory) throws Exception {
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(RESULT_QUEUE, false, false, false, null);

    System.out.println(" [*] Result Collector waiting for results");

    Map<String, Integer> finalResults = new HashMap<>();

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      String[] parts = message.split(":");
      if (parts.length == 2) {
        finalResults.put(parts[0], Integer.parseInt(parts[1]));
      }

      // 输出当前结果
      System.out.println("\n=== Current Results ===");
      List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(finalResults.entrySet());
      sortedEntries.sort(Map.Entry.<String, Integer>comparingByValue().reversed());

      for (Map.Entry<String, Integer> entry : sortedEntries) {
        System.out.println(entry.getKey() + ": " + entry.getValue());
      }
      System.out.println("======================\n");

      // 写入文件
      try (PrintWriter writer = new PrintWriter(new FileWriter("wordcount_results.txt"))) {
        for (Map.Entry<String, Integer> entry : sortedEntries) {
          writer.println(entry.getKey() + "\t" + entry.getValue());
        }
      } catch (IOException e) {
        System.err.println("Error writing results to file: " + e.getMessage());
      }
    };

    channel.basicConsume(RESULT_QUEUE, true, deliverCallback, consumerTag -> { });
  }

  // 提交输入文件
  public static void submitInputFile(ConnectionFactory factory, String filePath) throws Exception {
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(MAP_QUEUE, false, false, false, null);

    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        channel.basicPublish("", MAP_QUEUE, null, line.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent line: " + line);
      }
    }

    System.out.println(" [x] Input file submitted: " + filePath);
    connection.close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: java WordCount [map|reduce|result|submit <file>]");
      System.exit(1);
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

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
      default:
        System.err.println("Unknown command: " + command);
        System.exit(1);
    }
  }
}