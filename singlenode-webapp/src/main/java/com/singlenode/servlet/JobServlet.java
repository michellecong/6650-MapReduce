package com.singlenode.servlet;

import com.singlenode.dao.WordCountDao;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.singlenode.dao.JobDao;
import com.singlenode.model.Job;
import com.singlenode.util.DatabaseConfig;

/**
 * 处理作业提交和查询的Servlet，兼容MapReduceLoadTester
 */
@WebServlet(name = "JobServlet", urlPatterns = {
    "/api/jobs",
    "/api/jobs/*",
    "/api/jobs/*/wordcount",
    "/api/jobs/*/execution-time"
})
public class JobServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(JobServlet.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String UPLOAD_DIR = System.getProperty("java.io.tmpdir") + "/singlenode_uploads";

  @Override
  public void init() throws ServletException {
    try {
      // 初始化数据库
      DatabaseConfig.initialize();

      // 创建表
      JobDao.createJobTable();
      WordCountDao.createWordCountTable();

      // 创建上传目录
      Path uploadPath = Paths.get(UPLOAD_DIR);
      Files.createDirectories(uploadPath);

      logger.info("JobServlet initialized successfully");
      logger.info("Upload directory: {}", UPLOAD_DIR);
    } catch (Exception e) {
      logger.error("Error initializing JobServlet", e);
      throw new ServletException("Error initializing JobServlet", e);
    }
  }

  @Override
  public void destroy() {
    try {
      // 关闭数据库连接
      DatabaseConfig.close();
      logger.info("JobServlet destroyed");
    } catch (Exception e) {
      logger.error("Error destroying JobServlet", e);
    }
    super.destroy();
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String pathInfo = request.getPathInfo();
    String servletPath = request.getServletPath();
    String requestURI = request.getRequestURI();

    try {
      if (requestURI.endsWith("/wordcount")) {
        // 获取词频统计结果
        String jobId = pathInfo.substring(1, pathInfo.indexOf("/wordcount"));
        getWordCount(jobId, response);
      } else if (requestURI.endsWith("/execution-time")) {
        // 获取执行时间
        String jobId = pathInfo.substring(1, pathInfo.indexOf("/execution-time"));
        getExecutionTime(jobId, response);
      } else if (pathInfo == null || pathInfo.equals("/")) {
        // 获取所有作业
        getAllJobs(response);
      } else {
        // 获取单个作业状态
        String jobId = pathInfo.substring(1);
        getJob(jobId, response);
      }
    } catch (Exception e) {
      logger.error("Error processing GET request: {}", e.getMessage(), e);
      sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Error processing request: " + e.getMessage());
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      // 检查内容类型
      String contentType = request.getContentType();
      if (contentType != null && contentType.startsWith("application/json")) {
        submitJobFromJson(request, response);
      } else {
        sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
            "Unsupported content type. Please use application/json");
      }
    } catch (Exception e) {
      logger.error("Error processing POST request: {}", e.getMessage(), e);
      sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Error processing request: " + e.getMessage());
    }
  }

  /**
   * 从JSON请求提交作业
   */
  private void submitJobFromJson(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    // 读取并解析JSON请求体
    String requestBody = IOUtils.toString(request.getReader());
    JsonNode jsonNode = objectMapper.readTree(requestBody);

    // 提取必要信息
    String text = jsonNode.path("text").asText("");
    // 获取blob URL（如果存在）
    String blobUrl = null;
    if (!jsonNode.path("blobUrl").isMissingNode() && !jsonNode.path("blobUrl").isNull()) {
      blobUrl = jsonNode.path("blobUrl").asText();
    }

    // 如果同时没有提供text和blobUrl，则返回错误
    if (text.isEmpty() && (blobUrl == null || blobUrl.isEmpty())) {
      sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
          "Either text content or blob URL is required");
      return;
    }

    String inputBlobUrl = blobUrl;

    // 如果没有提供blobUrl但提供了text，创建临时文件并使用本地文件路径
    if ((blobUrl == null || blobUrl.isEmpty()) && !text.isEmpty()) {
      String fileName = "input_" + UUID.randomUUID() + ".txt";
      String filePath = UPLOAD_DIR + "/" + fileName;
      Files.write(Paths.get(filePath), text.getBytes());
      inputBlobUrl = filePath; // 使用本地文件路径作为blobUrl
    }

    // 创建作业
    String jobId = JobDao.createJob(inputBlobUrl);

    // 如果有输入数据，启动词频统计处理
    if (inputBlobUrl != null && !inputBlobUrl.isEmpty()) {
      processJob(jobId, inputBlobUrl);
    } else {
      logger.info("Job {} created without input data, waiting for external input", jobId);
    }

    // 返回作业ID
    ObjectNode result = objectMapper.createObjectNode();
    result.put("jobId", jobId);

    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try (PrintWriter out = response.getWriter()) {
      out.print(objectMapper.writeValueAsString(result));
    }
  }

  /**
   * 处理作业（词频统计）
   */
  private void processJob(String jobId, String blobUrl) {
    // 更新作业状态为运行中
    JobDao.updateJobStatus(jobId, Job.Status.RUNNING);

    // 创建新线程进行处理
    new Thread(() -> {
      try {
        // 执行词频统计
        Map<String, Integer> wordCounts = countWords(blobUrl);

        // 保存结果
        WordCountDao.saveWordCounts(jobId, wordCounts);

        // 生成输出文件
        String outputPath = UPLOAD_DIR + "/result_" + jobId + ".json";
        try (PrintWriter writer = new PrintWriter(outputPath)) {
          writer.print(objectMapper.writeValueAsString(wordCounts));
        }

        // 更新作业输出文件
        JobDao.updateJobOutputBlobUrl(jobId, outputPath);

        // 更新作业状态为已完成
        JobDao.updateJobStatus(jobId, Job.Status.COMPLETED);

        logger.info("Job {} completed successfully", jobId);
      } catch (Exception e) {
        logger.error("Error processing job: {}", jobId, e);
        JobDao.updateJobStatus(jobId, Job.Status.FAILED);
      }
    }).start();
  }

  /**
   * 词频统计
   */
  private Map<String, Integer> countWords(String filePath) throws IOException {
    Map<String, Integer> wordCounts = new HashMap<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // 简单拆分单词
        String[] words = line.toLowerCase().split("\\W+");

        for (String word : words) {
          if (word.length() > 0) {
            wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
          }
        }
      }
    }

    logger.info("Word count completed for file: {}, found {} unique words",
        filePath, wordCounts.size());
    return wordCounts;
  }

  /**
   * 获取所有作业
   */
  private void getAllJobs(HttpServletResponse response) throws IOException {
    List<Job> jobs = JobDao.getAllJobs();

    // 转换为符合期望格式的JSON结构
    ArrayNode jobsArray = objectMapper.createArrayNode();
    for (Job job : jobs) {
      ObjectNode jobNode = objectMapper.createObjectNode();
      jobNode.put("jobId", job.getJobId());
      jobNode.put("status", job.getStatus().name());

      // 处理可能为null的字段
      if (job.getInputBlobUrl() != null) {
        jobNode.put("inputBlobUrl", job.getInputBlobUrl());
      } else {
        jobNode.putNull("inputBlobUrl");
      }

      if (job.getOutputBlobUrl() != null) {
        jobNode.put("outputBlobUrl", job.getOutputBlobUrl());
      } else {
        jobNode.putNull("outputBlobUrl");
      }

      if (job.getCreatedAt() != null) {
        jobNode.put("createdAt", job.getCreatedAt().getTime());
      }
      if (job.getStartedAt() != null) {
        jobNode.put("startedAt", job.getStartedAt().getTime());
      }
      if (job.getCompletedAt() != null) {
        jobNode.put("completedAt", job.getCompletedAt().getTime());
      }

      jobsArray.add(jobNode);
    }

    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try (PrintWriter out = response.getWriter()) {
      out.print(objectMapper.writeValueAsString(jobsArray));
    }
  }

  /**
   * 获取单个作业
   */
  private void getJob(String jobId, HttpServletResponse response) throws IOException {
    Job job = JobDao.getJob(jobId);

    if (job == null) {
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Job not found: " + jobId);
      return;
    }

    // 创建期望格式的响应
    ObjectNode resultNode = objectMapper.createObjectNode();
    ObjectNode jobNode = objectMapper.createObjectNode();

    jobNode.put("jobId", job.getJobId());
    jobNode.put("status", job.getStatus().name());

    // 处理可能为null的字段
    if (job.getInputBlobUrl() != null) {
      jobNode.put("inputBlobUrl", job.getInputBlobUrl());
    } else {
      jobNode.putNull("inputBlobUrl");
    }

    if (job.getOutputBlobUrl() != null) {
      jobNode.put("outputBlobUrl", job.getOutputBlobUrl());
    } else {
      jobNode.putNull("outputBlobUrl");
    }

    if (job.getCreatedAt() != null) {
      jobNode.put("createdAt", job.getCreatedAt().getTime());
    }
    if (job.getStartedAt() != null) {
      jobNode.put("startedAt", job.getStartedAt().getTime());
    }
    if (job.getCompletedAt() != null) {
      jobNode.put("completedAt", job.getCompletedAt().getTime());
    }

    resultNode.set("job", jobNode);

    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try (PrintWriter out = response.getWriter()) {
      out.print(objectMapper.writeValueAsString(resultNode));
    }
  }

  /**
   * 获取词频统计结果
   */
  private void getWordCount(String jobId, HttpServletResponse response) throws IOException {
    Job job = JobDao.getJob(jobId);

    if (job == null) {
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Job not found: " + jobId);
      return;
    }

    if (job.getStatus() != Job.Status.COMPLETED) {
      sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
          "Job not completed yet: " + jobId);
      return;
    }

    // 获取词频统计
    Map<String, Integer> wordCounts = WordCountDao.getWordCounts(jobId);

    // 创建期望格式的响应
    ObjectNode resultNode = objectMapper.createObjectNode();
    resultNode.put("totalUniqueWords", wordCounts.size());

    int totalWordFrequency = 0;
    for (int count : wordCounts.values()) {
      totalWordFrequency += count;
    }
    resultNode.put("totalWordFrequency", totalWordFrequency);

    // 构建词频统计数组
    ArrayNode wordCountsArray = resultNode.putArray("wordCounts");
    for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
      ObjectNode wordCountNode = wordCountsArray.addObject();
      wordCountNode.put("word", entry.getKey());
      wordCountNode.put("count", entry.getValue());
    }

    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try (PrintWriter out = response.getWriter()) {
      out.print(objectMapper.writeValueAsString(resultNode));
    }
  }

  /**
   * 获取执行时间
   */
  private void getExecutionTime(String jobId, HttpServletResponse response) throws IOException {
    Job job = JobDao.getJob(jobId);

    if (job == null) {
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Job not found: " + jobId);
      return;
    }

    // 计算执行时间（毫秒）
    long executionTimeMs = 0;
    if (job.getStartedAt() != null && job.getCompletedAt() != null) {
      executionTimeMs = job.getCompletedAt().getTime() - job.getStartedAt().getTime();
    }

    // 构建响应
    ObjectNode resultNode = objectMapper.createObjectNode();
    resultNode.put("jobId", job.getJobId());
    resultNode.put("executionTimeMs", executionTimeMs);

    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try (PrintWriter out = response.getWriter()) {
      out.print(objectMapper.writeValueAsString(resultNode));
    }
  }

  /**
   * 发送错误响应
   */
  private void sendErrorResponse(HttpServletResponse response, int status, String message)
      throws IOException {
    response.setStatus(status);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");

    ObjectNode error = objectMapper.createObjectNode();
    error.put("error", message);

    try (PrintWriter out = response.getWriter()) {
      out.print(objectMapper.writeValueAsString(error));
    }
  }
}