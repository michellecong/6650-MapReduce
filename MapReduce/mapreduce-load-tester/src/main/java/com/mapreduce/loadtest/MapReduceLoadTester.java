package com.mapreduce.loadtest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapreduce.loadtest.TestFileLoader.TestFile;

/**
 * Load tester for MapReduce Web Application
 */
public class MapReduceLoadTester {
  private final String serverUri;
  private final HttpClient httpClient;
  private final ExecutorService executorService;
  private final ObjectMapper objectMapper;
  private final List<JobResult> jobResults;
  private final List<TestFile> testFiles;
  private final Map<String, Map<String, Integer>> expectedResults;
  private final Map<String, List<Long>> requestLatencies;
  private final AtomicInteger successfulRequests;
  private final AtomicInteger failedRequests;
  private long startTime;

  // Constants
  private static final int MAX_RETRIES = 3;
  private static final int POLLING_INTERVAL_MS = 2000;  // 2 seconds
  private static final int MAX_WAIT_TIME_MS = 300000;  // 5 minutes

  // Inner class to hold job result information
  public static class JobResult {
    private String jobId;
    private String fileName;
    private String status;
    private long executionTimeMs;
    private int numUniqueWords;
    private int totalWordFrequency;
    private double accuracy;
    private Map<String, Integer> wordCounts;

    public JobResult(String jobId, String fileName) {
      this.jobId = jobId;
      this.fileName = fileName;
      this.wordCounts = new HashMap<>();
    }

    // Getters and setters
    public String getJobId() { return jobId; }
    public String getFileName() { return fileName; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getExecutionTimeMs() { return executionTimeMs; }
    public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }
    public int getNumUniqueWords() { return numUniqueWords; }
    public void setNumUniqueWords(int numUniqueWords) { this.numUniqueWords = numUniqueWords; }
    public int getTotalWordFrequency() { return totalWordFrequency; }
    public void setTotalWordFrequency(int totalWordFrequency) { this.totalWordFrequency = totalWordFrequency; }
    public double getAccuracy() { return accuracy; }
    public void setAccuracy(double accuracy) { this.accuracy = accuracy; }
    public Map<String, Integer> getWordCounts() { return wordCounts; }
    public void setWordCounts(Map<String, Integer> wordCounts) { this.wordCounts = wordCounts; }

    @Override
    public String toString() {
      return String.format("Job ID: %s, File: %s, Status: %s, Execution Time: %d ms, Accuracy: %.2f%%",
          jobId, fileName, status, executionTimeMs, accuracy * 100);
    }
  }

  /**
   * Constructor that uses test files from a directory
   */
  public MapReduceLoadTester(String serverUri, String testFilesDirectory) throws IOException {
    this.serverUri = serverUri;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.executorService = Executors.newCachedThreadPool();
    this.objectMapper = new ObjectMapper();
    this.jobResults = Collections.synchronizedList(new ArrayList<>());
    this.requestLatencies = new ConcurrentHashMap<>();
    this.successfulRequests = new AtomicInteger(0);
    this.failedRequests = new AtomicInteger(0);

    // Load test files from directory
    TestFileLoader fileLoader = new TestFileLoader(testFilesDirectory);
    this.testFiles = fileLoader.getTestFiles();
    this.expectedResults = fileLoader.getExpectedResults();

    if (testFiles.isEmpty()) {
      System.out.println("Warning: No test files found in directory " + testFilesDirectory);
      System.out.println("Using built-in sample test files instead.");
      initializeTestFiles();
    }
  }

  /**
   * Constructor with default built-in test files
   */
  public MapReduceLoadTester(String serverUri) {
    this.serverUri = serverUri;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.executorService = Executors.newCachedThreadPool();
    this.objectMapper = new ObjectMapper();
    this.jobResults = Collections.synchronizedList(new ArrayList<>());
    this.testFiles = new ArrayList<>();
    this.expectedResults = new HashMap<>();
    this.requestLatencies = new ConcurrentHashMap<>();
    this.successfulRequests = new AtomicInteger(0);
    this.failedRequests = new AtomicInteger(0);

    // Initialize with some built-in test files
    initializeTestFiles();
  }

  /**
   * Initialize built-in test files and expected results
   */
  private void initializeTestFiles() {
    // Example test files with small content
    String file1Content = "This is a test file for MapReduce. This file contains simple words for counting.";
    String file2Content = "MapReduce is a programming model and an associated implementation for processing and generating big data sets with a parallel, distributed algorithm on a cluster.";
    String file3Content = "The term MapReduce actually refers to two separate and distinct tasks: Map and Reduce. The Map task takes a set of data and converts it into another set of data, where individual elements are broken down into tuples (key/value pairs).";

    testFiles.add(new TestFile("simple.txt", file1Content));
    testFiles.add(new TestFile("definition.txt", file2Content));
    testFiles.add(new TestFile("explanation.txt", file3Content));

    // Add expected word counts for testing accuracy
    Map<String, Integer> expectedFile1 = new HashMap<>();
    expectedFile1.put("this", 2);
    expectedFile1.put("is", 1);
    expectedFile1.put("a", 1);
    expectedFile1.put("test", 1);
    expectedFile1.put("file", 2);
    expectedFile1.put("for", 2);
    expectedFile1.put("mapreduce", 1);
    expectedFile1.put("contains", 1);
    expectedFile1.put("simple", 1);
    expectedFile1.put("words", 1);
    expectedFile1.put("counting", 1);

    Map<String, Integer> expectedFile2 = new HashMap<>();
    expectedFile2.put("mapreduce", 1);
    expectedFile2.put("is", 1);
    expectedFile2.put("a", 3);
    expectedFile2.put("programming", 1);
    expectedFile2.put("model", 1);
    expectedFile2.put("and", 2);
    expectedFile2.put("an", 1);
    expectedFile2.put("associated", 1);
    expectedFile2.put("implementation", 1);
    expectedFile2.put("for", 1);
    expectedFile2.put("processing", 1);
    expectedFile2.put("generating", 1);
    expectedFile2.put("big", 1);
    expectedFile2.put("data", 1);
    expectedFile2.put("sets", 1);
    expectedFile2.put("with", 1);
    expectedFile2.put("parallel", 1);
    expectedFile2.put("distributed", 1);
    expectedFile2.put("algorithm", 1);
    expectedFile2.put("on", 1);
    expectedFile2.put("cluster", 1);

    Map<String, Integer> expectedFile3 = new HashMap<>();
    expectedFile3.put("the", 2);
    expectedFile3.put("term", 1);
    expectedFile3.put("mapreduce", 1);
    expectedFile3.put("actually", 1);
    expectedFile3.put("refers", 1);
    expectedFile3.put("to", 1);
    expectedFile3.put("two", 1);
    expectedFile3.put("separate", 1);
    expectedFile3.put("and", 3);
    expectedFile3.put("distinct", 1);
    expectedFile3.put("tasks", 1);
    expectedFile3.put("map", 2);
    expectedFile3.put("reduce", 1);
    expectedFile3.put("task", 1);
    expectedFile3.put("takes", 1);
    expectedFile3.put("a", 1);
    expectedFile3.put("set", 2);
    expectedFile3.put("of", 2);
    expectedFile3.put("data", 2);
    expectedFile3.put("converts", 1);
    expectedFile3.put("it", 1);
    expectedFile3.put("into", 2);
    expectedFile3.put("another", 1);
    expectedFile3.put("where", 1);
    expectedFile3.put("individual", 1);
    expectedFile3.put("elements", 1);
    expectedFile3.put("are", 1);
    expectedFile3.put("broken", 1);
    expectedFile3.put("down", 1);
    expectedFile3.put("tuples", 1);
    expectedFile3.put("key", 1);
    expectedFile3.put("value", 1);
    expectedFile3.put("pairs", 1);

// 将预期结果添加到映射中
    expectedResults.put("explanation.txt", expectedFile3);
    // Add the expected results to the map
    expectedResults.put("simple.txt", expectedFile1);
    expectedResults.put("definition.txt", expectedFile2);
    // For explanation.txt, we would compute or define expected results similarly
  }

  /**
   * Run load test with multiple concurrent users
   */
  public void runLoadTest(int numUsers, int requestsPerUser) throws Exception {
    System.out.println("Starting MapReduce load test with " + numUsers + " users, " + requestsPerUser + " requests per user");
    startTime = System.currentTimeMillis();

    CountDownLatch latch = new CountDownLatch(numUsers);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numUsers; i++) {
      final int userId = i;
      Future<?> future = executorService.submit(() -> {
        try {
          for (int j = 0; j < requestsPerUser; j++) {
            // Randomly select a test file
            TestFile testFile = getRandomTestFile();

            // Submit job and get job ID
            String jobId = submitJob(testFile.getContent(), testFile.getFileName(), 5, true);
            if (jobId != null) {
              JobResult jobResult = new JobResult(jobId, testFile.getFileName());
              jobResults.add(jobResult);

              // Poll job status until completion
              boolean jobCompleted = waitForJobCompletion(jobId, jobResult);

              // If job completed successfully, get word counts and calculate accuracy
              if (jobCompleted) {
                getWordCounts(jobId, jobResult);

                // Calculate execution time
                getJobExecutionTime(jobId, jobResult);

                // Calculate accuracy if expected results exist
                if (expectedResults.containsKey(testFile.getFileName())) {
                  calculateAccuracy(jobResult, expectedResults.get(testFile.getFileName()));
                }
              }
            }
          }
        } catch (Exception e) {
          System.err.println("Error in user thread " + userId + ": " + e.getMessage());
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
      futures.add(future);
    }

    // Wait for all users to complete
    latch.await();

    // Summarize results
    summarizeResults();

    // Write results to CSV
    writeResultsToCSV();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  /**
   * Get a random test file
   */
  private TestFile getRandomTestFile() {
    if (testFiles.isEmpty()) {
      return null;
    }
    int randomIndex = (int) (Math.random() * testFiles.size());
    return testFiles.get(randomIndex);
  }

  /**
   * Submit a job to the MapReduce application
   */
  private String submitJob(String textContent, String fileName, int numReduceTasks, boolean useBlob) throws Exception {
    String jobId = null;
    long startRequestTime = System.currentTimeMillis();

    try {
      // Prepare JSON request payload
      Map<String, Object> requestData = new HashMap<>();
      requestData.put("text", textContent);
      requestData.put("fileName", fileName);
      requestData.put("numReduceTasks", numReduceTasks);
      requestData.put("useBlob", useBlob);

      String requestBody = objectMapper.writeValueAsString(requestData);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(serverUri + "/api/jobs"))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(requestBody))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      long endRequestTime = System.currentTimeMillis();
      long latency = endRequestTime - startRequestTime;

      // Record latency
      recordLatency("SUBMIT_JOB", latency);

      if (response.statusCode() >= 200 && response.statusCode() < 300) {
        successfulRequests.incrementAndGet();

        // Parse response to get job ID
        JsonNode rootNode = objectMapper.readTree(response.body());
        jobId = rootNode.path("jobId").asText();

        System.out.println("Job submitted successfully: " + jobId);
      } else {
        failedRequests.incrementAndGet();
        System.err.println("Failed to submit job. Status code: " + response.statusCode());
        System.err.println("Response body: " + response.body());
      }
    } catch (Exception e) {
      failedRequests.incrementAndGet();
      System.err.println("Error submitting job: " + e.getMessage());
      throw e;
    }

    return jobId;
  }

  /**
   * Poll job status until completion or timeout
   */
  private boolean waitForJobCompletion(String jobId, JobResult jobResult) throws Exception {
    long startWaitTime = System.currentTimeMillis();
    boolean completed = false;

    while (System.currentTimeMillis() - startWaitTime < MAX_WAIT_TIME_MS) {
      long startRequestTime = System.currentTimeMillis();

      try {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(serverUri + "/api/jobs/" + jobId))
            .GET()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        long endRequestTime = System.currentTimeMillis();
        long latency = endRequestTime - startRequestTime;

        // Record latency
        recordLatency("GET_JOB_STATUS", latency);

        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          successfulRequests.incrementAndGet();

          // Parse response
          JsonNode rootNode = objectMapper.readTree(response.body());
          JsonNode jobNode = rootNode.path("job");
          String status = jobNode.path("status").asText();

          jobResult.setStatus(status);

          if ("COMPLETED".equals(status)) {
            System.out.println("Job " + jobId + " completed successfully");
            completed = true;
            break;
          } else if ("FAILED".equals(status)) {
            System.out.println("Job " + jobId + " failed");
            break;
          }

          // Wait before polling again
          Thread.sleep(POLLING_INTERVAL_MS);
        } else {
          failedRequests.incrementAndGet();
          System.err.println("Failed to get job status. Status code: " + response.statusCode());
          break;
        }
      } catch (Exception e) {
        failedRequests.incrementAndGet();
        System.err.println("Error getting job status: " + e.getMessage());
        throw e;
      }
    }

    if (!completed && !"FAILED".equals(jobResult.getStatus())) {
      jobResult.setStatus("TIMEOUT");
      System.out.println("Timed out waiting for job " + jobId + " to complete");
    }

    return completed;
  }

  /**
   * Get word counts for a completed job
   */
  private void getWordCounts(String jobId, JobResult jobResult) throws Exception {
    long startRequestTime = System.currentTimeMillis();

    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(serverUri + "/api/jobs/" + jobId + "/wordcount"))
          .GET()
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      long endRequestTime = System.currentTimeMillis();
      long latency = endRequestTime - startRequestTime;

      // Record latency
      recordLatency("GET_WORD_COUNTS", latency);

      if (response.statusCode() >= 200 && response.statusCode() < 300) {
        successfulRequests.incrementAndGet();

        // Parse response
        JsonNode rootNode = objectMapper.readTree(response.body());

        // Get summary statistics
        jobResult.setNumUniqueWords(rootNode.path("totalUniqueWords").asInt());
        jobResult.setTotalWordFrequency(rootNode.path("totalWordFrequency").asInt());

        // Get individual word counts
        JsonNode wordCountsNode = rootNode.path("wordCounts");
        Map<String, Integer> wordCounts = new HashMap<>();

        if (wordCountsNode.isArray()) {
          for (JsonNode wordCountNode : wordCountsNode) {
            String word = wordCountNode.path("word").asText();
            int count = wordCountNode.path("count").asInt();
            wordCounts.put(word, count);
          }
        }

        jobResult.setWordCounts(wordCounts);
      } else {
        failedRequests.incrementAndGet();
        System.err.println("Failed to get word counts. Status code: " + response.statusCode());
      }
    } catch (Exception e) {
      failedRequests.incrementAndGet();
      System.err.println("Error getting word counts: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Get job execution time
   */
  private void getJobExecutionTime(String jobId, JobResult jobResult) throws Exception {
    long startRequestTime = System.currentTimeMillis();

    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(serverUri + "/api/jobs/" + jobId + "/execution-time"))
          .GET()
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      long endRequestTime = System.currentTimeMillis();
      long latency = endRequestTime - startRequestTime;

      // Record latency
      recordLatency("GET_EXECUTION_TIME", latency);

      if (response.statusCode() >= 200 && response.statusCode() < 300) {
        successfulRequests.incrementAndGet();

        // Parse response
        JsonNode rootNode = objectMapper.readTree(response.body());

        // Check if executionTimeMs is present
        if (rootNode.has("executionTimeMs")) {
          jobResult.setExecutionTimeMs(rootNode.path("executionTimeMs").asLong());
        }
      } else {
        failedRequests.incrementAndGet();
        System.err.println("Failed to get execution time. Status code: " + response.statusCode());
      }
    } catch (Exception e) {
      failedRequests.incrementAndGet();
      System.err.println("Error getting execution time: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Calculate accuracy by comparing with expected results
   */
  private void calculateAccuracy(JobResult jobResult, Map<String, Integer> expectedWordCounts) {
    int correctCounts = 0;
    int totalWords = expectedWordCounts.size();

    // Get the actual word counts from the job result
    Map<String, Integer> actualWordCounts = jobResult.getWordCounts();

    // Count how many words have the correct count
    for (Map.Entry<String, Integer> entry : expectedWordCounts.entrySet()) {
      String word = entry.getKey();
      int expectedCount = entry.getValue();

      if (actualWordCounts.containsKey(word) && actualWordCounts.get(word) == expectedCount) {
        correctCounts++;
      }
    }

    // Calculate accuracy as percentage of correct counts
    double accuracy = (double) correctCounts / totalWords;
    jobResult.setAccuracy(accuracy);
  }

  /**
   * Record latency for a request type
   */
  private void recordLatency(String requestType, long latency) {
    requestLatencies.computeIfAbsent(requestType, k -> Collections.synchronizedList(new ArrayList<>())).add(latency);
  }

  /**
   * Summarize test results
   */
  private void summarizeResults() {
    long endTime = System.currentTimeMillis();
    double totalDuration = (endTime - startTime) / 1000.0;
    int totalRequests = successfulRequests.get() + failedRequests.get();
    double throughput = totalRequests / totalDuration;

    System.out.println("\n========== TEST SUMMARY ==========");
    System.out.println("Total Duration: " + String.format("%.2f seconds", totalDuration));
    System.out.println("Total Requests: " + totalRequests);
    System.out.println("Successful Requests: " + successfulRequests.get());
    System.out.println("Failed Requests: " + failedRequests.get());
    System.out.println("Throughput: " + String.format("%.2f requests/second", throughput));

    // Calculate and print latency statistics for each request type
    for (Map.Entry<String, List<Long>> entry : requestLatencies.entrySet()) {
      String requestType = entry.getKey();
      List<Long> latencies = entry.getValue();

      if (!latencies.isEmpty()) {
        Collections.sort(latencies);

        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);
        long median = latencies.get(latencies.size() / 2);
        double mean = latencies.stream().mapToLong(l -> l).average().orElse(0.0);
        long p90 = latencies.get((int) (latencies.size() * 0.9));

        System.out.println("\n" + requestType + " Latency Statistics:");
        System.out.println("Count: " + latencies.size());
        System.out.println("Min: " + min + "ms");
        System.out.println("Max: " + max + "ms");
        System.out.println("Mean: " + String.format("%.2f ms", mean));
        System.out.println("Median: " + median + "ms");
        System.out.println("90th Percentile: " + p90 + "ms");
      }
    }

    // Print job results
    System.out.println("\n========== JOB RESULTS ==========");

    // Sort by execution time
    List<JobResult> sortedResults = new ArrayList<>(jobResults);
    sortedResults.sort(Comparator.comparingLong(JobResult::getExecutionTimeMs));

    for (JobResult result : sortedResults) {
      if ("COMPLETED".equals(result.getStatus())) {
        System.out.println(result);
      }
    }

    // Calculate accuracy statistics
    List<Double> accuracies = jobResults.stream()
        .filter(r -> "COMPLETED".equals(r.getStatus()))
        .map(JobResult::getAccuracy)
        .filter(a -> a > 0) // Only include jobs where accuracy was calculated
        .collect(Collectors.toList());

    if (!accuracies.isEmpty()) {
      double minAccuracy = accuracies.stream().min(Double::compare).orElse(0.0);
      double maxAccuracy = accuracies.stream().max(Double::compare).orElse(0.0);
      double avgAccuracy = accuracies.stream().mapToDouble(a -> a).average().orElse(0.0);

      System.out.println("\nAccuracy Statistics:");
      System.out.println("Min Accuracy: " + String.format("%.2f%%", minAccuracy * 100));
      System.out.println("Max Accuracy: " + String.format("%.2f%%", maxAccuracy * 100));
      System.out.println("Average Accuracy: " + String.format("%.2f%%", avgAccuracy * 100));
    }
  }

  /**
   * Write test results to CSV files
   */
  private void writeResultsToCSV() throws IOException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
    String timestamp = dateFormat.format(new Date());

    // Write job results
    String jobResultsFile = "job_results_" + timestamp + ".csv";
    try (FileWriter writer = new FileWriter(jobResultsFile)) {
      writer.write("JobID,FileName,Status,ExecutionTime(ms),UniqueWords,TotalFrequency,Accuracy\n");

      for (JobResult result : jobResults) {
        writer.write(String.format("%s,%s,%s,%d,%d,%d,%.4f\n",
            result.getJobId(),
            result.getFileName(),
            result.getStatus(),
            result.getExecutionTimeMs(),
            result.getNumUniqueWords(),
            result.getTotalWordFrequency(),
            result.getAccuracy()));
      }
    }

    // Write latency results
    String latencyResultsFile = "latency_results_" + timestamp + ".csv";
    try (FileWriter writer = new FileWriter(latencyResultsFile)) {
      writer.write("RequestType,Latency(ms)\n");

      for (Map.Entry<String, List<Long>> entry : requestLatencies.entrySet()) {
        String requestType = entry.getKey();
        List<Long> latencies = entry.getValue();

        for (Long latency : latencies) {
          writer.write(String.format("%s,%d\n", requestType, latency));
        }
      }
    }

    System.out.println("\nResults written to:");
    System.out.println("- " + jobResultsFile);
    System.out.println("- " + latencyResultsFile);
  }

  /**
   * Main method
   */
  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("Usage: java com.mapreduce.loadtest.MapReduceLoadTester <serverUri> <numUsers> <requestsPerUser> [testFilesDirectory]");
      System.out.println("Example: java com.mapreduce.loadtest.MapReduceLoadTester http://localhost:8080 10 5 ./test-files");
      System.exit(1);
    }

    String serverUri = args[0];
    int numUsers = Integer.parseInt(args[1]);
    int requestsPerUser = Integer.parseInt(args[2]);
    String testFilesDirectory = args.length > 3 ? args[3] : null;

    try {
      MapReduceLoadTester loadTester;
      if (testFilesDirectory != null) {
        System.out.println("Loading test files from directory: " + testFilesDirectory);
        loadTester = new MapReduceLoadTester(serverUri, testFilesDirectory);
      } else {
        System.out.println("Using built-in test files");
        loadTester = new MapReduceLoadTester(serverUri);
      }

      loadTester.runLoadTest(numUsers, requestsPerUser);
    } catch (Exception e) {
      System.err.println("Error running load test: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}