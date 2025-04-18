package com.mapreduce.worker;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.KeyValue;
import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.messaging.MessageProducer;
import com.mapreduce.storage.BlobStorageService;

/**
 * Map 任务处理器 - 完全使用Blob存储
 */
public class MapWorker {
    private static final Logger logger = LogManager.getLogger(MapWorker.class);
    
    private final MessageProducer messageProducer;
    private final String workerId;
    private final int numReduceTasks;
    private final String baseDir;
    private final BlobStorageService blobStorageService;
    
    /**
     * 创建 Map 任务处理器
     */
    public MapWorker(MessageProducer messageProducer, String workerId, int numReduceTasks, 
                    String baseDir, BlobStorageService blobStorageService) {
        this.messageProducer = messageProducer;
        this.workerId = workerId;
        this.numReduceTasks = numReduceTasks;
        this.baseDir = baseDir;
        this.blobStorageService = blobStorageService;
    }
    
    /**
     * 执行 Map 任务
     */
    public void execute(Task task) throws IOException {
        String taskId = task.getTaskId();
        String jobId = task.getJobId();
        String inputPath = task.getInputPath();
        String inputBlobUrl = task.getInputBlobUrl();
        StorageType storageType = task.getStorageType();
        
        // 强制使用Blob存储
        if (storageType != StorageType.BLOB || inputBlobUrl == null || inputBlobUrl.isEmpty()) {
            logger.warn("Task not properly configured for Blob storage. Checking if we can continue.");
            if (inputBlobUrl != null && !inputBlobUrl.isEmpty()) {
                logger.info("Task has inputBlobUrl, continuing with Blob storage mode");
                storageType = StorageType.BLOB;
                task.setStorageType(StorageType.BLOB);
            } else if (inputPath != null && !inputPath.isEmpty() && blobStorageService.isEnabled()) {
                logger.warn("No inputBlobUrl but have inputPath. Will upload to Blob first.");
                try {
                    String blobName = "input/" + jobId + "/" + new File(inputPath).getName();
                    inputBlobUrl = blobStorageService.uploadFile(inputPath, blobName);
                    if (inputBlobUrl != null) {
                        logger.info("Successfully uploaded input file to Blob: {}", inputBlobUrl);
                        task.setInputBlobUrl(inputBlobUrl);
                        storageType = StorageType.BLOB;
                        task.setStorageType(StorageType.BLOB);
                    } else {
                        throw new IOException("Failed to upload input file to Blob");
                    }
                } catch (Exception e) {
                    logger.error("Error uploading input file to Blob", e);
                    throw new IOException("Error uploading input file to Blob", e);
                }
            } else {
                throw new IOException("Blob storage is required but not configured for this task");
            }
        }
        
        logger.info("Executing map task: {}, using Blob storage", taskId);
        
        // 创建临时目录
        String tempDir = baseDir + "/temp";
        Files.createDirectories(Paths.get(tempDir));
        
        // 从Blob下载输入文件到临时目录
        String tempInputPath = tempDir + "/input_" + taskId + "_" + System.currentTimeMillis() + ".txt";
        
        try {
            // 下载输入文件
            logger.info("Downloading input file from Blob: {} -> {}", inputBlobUrl, tempInputPath);
            
            // 确保目录存在
            Files.createDirectories(Paths.get(tempInputPath).getParent());
            
            // 获取Blob名称
            String blobName = blobStorageService.getBlobNameFromUrl(inputBlobUrl);
            if (blobName == null) {
                throw new IOException("Failed to extract blob name from URL: " + inputBlobUrl);
            }
            
            blobStorageService.downloadFile(blobName, tempInputPath);
            
            // 检查文件大小
            File tempFile = new File(tempInputPath);
            if (!tempFile.exists()) {
                throw new IOException("Downloaded file does not exist: " + tempInputPath);
            }
            
            if (tempFile.length() == 0) {
                logger.warn("Downloaded file is empty: {}", tempInputPath);
            }
            
            logger.info("Successfully downloaded input file: {} bytes", tempFile.length());
            
            // 读取输入文件并计算词频
            Map<String, Integer> wordCounts = countWords(tempInputPath);
            logger.info("Word counting completed, found {} unique words", wordCounts.size());
            
            // 显示一些词汇用于调试
            if (wordCounts.size() > 0) {
                logger.info("Sample words: {}", 
                    wordCounts.entrySet().stream()
                        .limit(10)
                        .map(e -> e.getKey() + ":" + e.getValue())
                        .reduce((s1, s2) -> s1 + ", " + s2)
                        .orElse("(none)"));
            }
            
            // 按分区分组结果
            Map<Integer, List<KeyValue>> partitions = partitionMapOutput(wordCounts);
            
            // 将中间结果直接上传到Blob，不保存本地文件
            List<String> blobUrls = uploadIntermediateToBlob(jobId, taskId, partitions);
            
            // 发送结果消息（不再包含本地文件路径）
            logger.info("Sending map result with {} blob URLs", blobUrls.size());
            messageProducer.sendMapResult(taskId, jobId, null, blobUrls);
            
            logger.info("Map task completed: {}, processed {} words, wrote {} partition blobs", 
                      taskId, wordCounts.size(), blobUrls.size());
        } finally {
            // 清理临时文件
            try {
                Files.deleteIfExists(Paths.get(tempInputPath));
                logger.debug("Deleted temporary input file: {}", tempInputPath);
            } catch (Exception e) {
                logger.warn("Failed to delete temporary input file: {}", tempInputPath, e);
            }
        }
    }
    
    /**
     * 将中间结果直接上传到Blob存储
     */
    private List<String> uploadIntermediateToBlob(String jobId, String taskId,
                                                Map<Integer, List<KeyValue>> partitions) throws IOException {
        List<String> blobUrls = new ArrayList<>();
        
        // 删除添加测试词的代码 - 我们完全使用实际的单词统计结果
        if (partitions.isEmpty()) {
            logger.warn("No words found in input, creating empty partitions to continue processing");
            // 创建空分区但不添加测试词
            for (int i = 0; i < numReduceTasks; i++) {
                if (!partitions.containsKey(i)) {
                    partitions.put(i, new ArrayList<>());
                }
            }
        }
        
        for (Map.Entry<Integer, List<KeyValue>> entry : partitions.entrySet()) {
            int partitionId = entry.getKey();
            List<KeyValue> keyValues = entry.getValue();
            
            // 即使列表为空，也创建内容以确保所有分区都有对应的blob
            StringBuilder content = new StringBuilder();
            for (KeyValue kv : keyValues) {
                content.append(kv.getKey()).append("\t").append(kv.getValue()).append(System.lineSeparator());
            }
            
            // 检查内容
            String contentStr = content.toString();
            logger.debug("Partition {} content size: {} bytes", partitionId, contentStr.length());
            
            // 直接上传内容到Blob
            String blobName = "intermediate/" + jobId + "/" + taskId + "/part_" + partitionId + ".txt";
            String blobUrl = blobStorageService.uploadText(contentStr, blobName);
            
            if (blobUrl != null) {
                blobUrls.add(blobUrl);
                logger.info("Uploaded intermediate content to Blob: partition {} -> {}", partitionId, blobUrl);
            } else {
                logger.error("Failed to upload intermediate content to Blob: partition {}", partitionId);
            }
        }
        
        return blobUrls;
    }
    
    /**
     * 将 Map 输出按分区分组
     */
    private Map<Integer, List<KeyValue>> partitionMapOutput(Map<String, Integer> wordCounts) {
        Map<Integer, List<KeyValue>> partitions = new HashMap<>();
        
        // 初始化分区
        for (int i = 0; i < numReduceTasks; i++) {
            partitions.put(i, new ArrayList<>());
        }
        
        // 按单词的哈希值将结果分配到不同分区
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            String word = entry.getKey();
            int count = entry.getValue();
            
            // 计算分区号，确保非负
            int hashCode = word.hashCode();
            int partitionId = Math.abs(hashCode % numReduceTasks);
            
            // 添加到对应分区
            partitions.get(partitionId).add(new KeyValue(word, count));
            
            // 记录分配（仅调试用）
            if (wordCounts.size() <= 20) {
                logger.debug("Word '{}' with count {} assigned to partition {}", word, count, partitionId);
            }
        }
        
        return partitions;
    }
    
    /**
     * 计算单词频率
     */
    private Map<String, Integer> countWords(String filePath) throws IOException {
        Map<String, Integer> wordCounts = new HashMap<>();
        long startTime = System.currentTimeMillis();
        int lineCount = 0;
        int wordCount = 0;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineCount++;
                
                // 记录原始行内容（仅调试用）
                if (lineCount <= 5) {
                    logger.debug("Processing line {}: '{}'", lineCount, line);
                }
                
                // 跳过空行
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                // 将文本转为小写并分割为单词
                String[] words = line.toLowerCase().split("\\W+");
                
                for (String word : words) {
                    if (word.isEmpty() || word.matches("^\\d+$")) {
                        continue; // 跳过空单词和纯数字
                    }
                    
                    wordCount++;
                    wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    
                    // 记录前50个单词（仅调试用）
                    if (wordCount <= 50) {
                        logger.debug("Counted word: '{}' -> {}", word, 
                                   wordCounts.get(word));
                    }
                    
                    // 每处理100万个单词记录一次日志
                    if (wordCount % 1000000 == 0) {
                        logger.info("Processed {} words from {} lines", wordCount, lineCount);
                    }
                }
                
                // 每处理10万行记录一次日志
                if (lineCount % 100000 == 0) {
                    logger.info("Processed {} lines", lineCount);
                }
            }
        }
        
        long endTime = System.currentTimeMillis();
        logger.info("Word counting completed in {}ms, processed {} lines, {} words, found {} unique words", 
                  (endTime - startTime), lineCount, wordCount, wordCounts.size());
        
        return wordCounts;
    }
}