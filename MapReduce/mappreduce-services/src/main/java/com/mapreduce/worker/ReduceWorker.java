package com.mapreduce.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.KeyValue;
import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.db.MapOutputDao;
import com.mapreduce.messaging.MessageProducer;
import com.mapreduce.storage.BlobStorageService;

/**
 * Reduce 任务处理器 - 完全使用Blob存储
 */
public class ReduceWorker {
    private static final Logger logger = LogManager.getLogger(ReduceWorker.class);
    
    private final MessageProducer messageProducer;
    private final String workerId;
    private final String baseDir;
    private final BlobStorageService blobStorageService;
    
    /**
     * 创建 Reduce 任务处理器
     */
    public ReduceWorker(MessageProducer messageProducer, String workerId, String baseDir, 
                       BlobStorageService blobStorageService) {
        this.messageProducer = messageProducer;
        this.workerId = workerId;
        this.baseDir = baseDir;
        this.blobStorageService = blobStorageService;
    }
    
    /**
     * 执行 Reduce 任务
     */
    public void execute(Task task) throws IOException, SQLException {
        String taskId = task.getTaskId();
        String jobId = task.getJobId();
        int partitionId = task.getPartitionId();
        StorageType storageType = task.getStorageType();
        
        // 修改：强制使用Blob存储，即使任务配置错误
        if (storageType != StorageType.BLOB) {
            logger.warn("Task is not configured for Blob storage. Forcing Blob storage mode.");
            storageType = StorageType.BLOB;
            task.setStorageType(StorageType.BLOB);
        }
        
        if (!blobStorageService.isEnabled()) {
            throw new IOException("Blob storage service is not enabled but required for operation");
        }
        
        logger.info("Executing reduce task: {}, partition: {}, using Blob storage", 
                  taskId, partitionId);
        
        // 获取该分区的所有中间文件Blob URLs
        List<MapOutputDao.MapOutput> mapOutputs = MapOutputDao.getMapOutputsForPartition(jobId, partitionId);
        
        logger.info("Found {} map outputs for partition {} of job {}", 
                  mapOutputs.size(), partitionId, jobId);
        
        // 打印所有找到的map输出，用于调试
        for (MapOutputDao.MapOutput output : mapOutputs) {
            logger.info("Map output: partition={}, filePath={}, blobUrl={}", 
                        output.getPartitionId(), output.getFilePath(), output.getBlobUrl());
        }
        
        // 检查是否有map输出
        if (mapOutputs.isEmpty()) {
            logger.warn("No map outputs found for partition {}, job {}", partitionId, jobId);
            
            // 创建一个空结果并上传到Blob
            String emptyBlobUrl = createEmptyOutputBlob(jobId, partitionId);
            
            // 发送任务完成消息
            messageProducer.sendReduceResult(taskId, jobId, null, emptyBlobUrl);
            logger.info("Reduce task completed (empty): {}", taskId);
            return;
        }
        
        // 合并所有键值对
        Map<String, Integer> wordCounts = new HashMap<>();
        int totalBlobs = 0;
        int processedBlobs = 0;
        
        // 首先统计有效的Blob URL数量
        for (MapOutputDao.MapOutput mapOutput : mapOutputs) {
            if (mapOutput.getBlobUrl() != null && !mapOutput.getBlobUrl().isEmpty()) {
                totalBlobs++;
            }
        }
        
        if (totalBlobs == 0) {
            logger.warn("No blob URLs found for partition {}, job {}", partitionId, jobId);
            
            // 创建一个空结果并上传到Blob
            String emptyBlobUrl = createEmptyOutputBlob(jobId, partitionId);
            
            // 发送任务完成消息
            messageProducer.sendReduceResult(taskId, jobId, null, emptyBlobUrl);
            logger.info("Reduce task completed (empty): {}", taskId);
            return;
        }
        
        logger.info("Starting to process {} intermediate blobs for partition {}, job {}", 
                  totalBlobs, partitionId, jobId);
        
        // 处理所有中间文件
        long startTime = System.currentTimeMillis();
        String tempDir = baseDir + "/temp";
        Files.createDirectories(Paths.get(tempDir));
        
        for (MapOutputDao.MapOutput mapOutput : mapOutputs) {
            String blobUrl = mapOutput.getBlobUrl();
            
            if (blobUrl != null && !blobUrl.isEmpty()) {
                String tempFilePath = tempDir + "/temp_" + jobId + "_" + partitionId + "_" + System.currentTimeMillis() + ".txt";
                try {
                    logger.debug("Downloading intermediate file from Blob: {} -> {}", blobUrl, tempFilePath);
                    
                    // 从Blob下载到临时文件
                    blobStorageService.downloadFile(blobStorageService.getBlobNameFromUrl(blobUrl), tempFilePath);
                    
                    // 读取下载的文件
                    List<KeyValue> keyValues = readIntermediateFile(tempFilePath);
                    mergeKeyValues(wordCounts, keyValues);
                    
                    // 删除临时文件
                    try {
                        Files.deleteIfExists(Paths.get(tempFilePath));
                    } catch (Exception e) {
                        logger.warn("Failed to delete temporary file: {}", tempFilePath, e);
                    }
                    
                    processedBlobs++;
                    logger.debug("Processed intermediate blob: {}, progress: {}/{}", 
                              blobUrl, processedBlobs, totalBlobs);
                } catch (Exception e) {
                    logger.error("Failed to process intermediate blob: {}", blobUrl, e);
                }
            }
            
            // 每处理10个文件记录一次进度
            if (processedBlobs % 10 == 0 || processedBlobs == totalBlobs) {
                logger.info("Reduce progress: processed {}/{} blobs, {} unique words", 
                          processedBlobs, totalBlobs, wordCounts.size());
            }
        }
        
        long processingTime = System.currentTimeMillis() - startTime;
        logger.info("Finished processing {} intermediate blobs in {}ms, {} unique words found", 
                  processedBlobs, processingTime, wordCounts.size());
        
        // 记录词频结果内容到日志，便于调试
        if (wordCounts.size() > 0) {
            logger.info("Word count summary (top 20): {}", 
                    wordCounts.entrySet().stream()
                        .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                        .limit(20)
                        .map(e -> e.getKey() + ":" + e.getValue())
                        .reduce((s1, s2) -> s1 + ", " + s2)
                        .orElse("(none)"));
        } else {
            logger.warn("No words counted in task: {}", taskId);
        }
        
        // 将结果直接上传到Blob
        String outputBlobUrl = uploadReduceOutputToBlob(jobId, partitionId, wordCounts);
        
        // 发送任务完成消息
        messageProducer.sendReduceResult(taskId, jobId, null, outputBlobUrl);
        
        logger.info("Reduce task completed: {}, processed {} words", taskId, wordCounts.size());
    }
    
    /**
     * 合并键值对到结果Map
     */
    private void mergeKeyValues(Map<String, Integer> wordCounts, List<KeyValue> keyValues) {
        for (KeyValue kv : keyValues) {
            String word = kv.getKey();
            int count = kv.getValue();
            
            if (word != null && !word.isEmpty()) {
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
            }
        }
    }
    
    /**
     * 创建空的输出Blob
     */
    private String createEmptyOutputBlob(String jobId, int partitionId) throws IOException {
        String blobName = "output/" + jobId + "/reduce_" + partitionId + ".txt";
        return blobStorageService.uploadText("", blobName);
    }
    
    /**
     * 上传Reduce输出到Blob存储
     */
    private String uploadReduceOutputToBlob(String jobId, int partitionId, Map<String, Integer> wordCounts) throws IOException {
        // 创建内容字符串
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            content.append(entry.getKey()).append("\t").append(entry.getValue()).append(System.lineSeparator());
        }
        
        // 上传到Blob
        String blobName = "output/" + jobId + "/reduce_" + partitionId + ".txt";
        String blobUrl = blobStorageService.uploadText(content.toString(), blobName);
        
        if (blobUrl != null) {
            logger.info("Uploaded reduce output to Blob: {}", blobUrl);
        } else {
            logger.error("Failed to upload reduce output to Blob");
        }
        
        return blobUrl;
    }
    
    /**
     * 读取中间文件
     */
    private List<KeyValue> readIntermediateFile(String filePath) throws IOException {
        List<KeyValue> keyValues = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 跳过空行
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                String[] parts = line.split("\\t");
                if (parts.length == 2) {
                    try {
                        String key = parts[0];
                        int value = Integer.parseInt(parts[1]);
                        keyValues.add(new KeyValue(key, value));
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid format in line: {} ({})", line, e.getMessage());
                    }
                } else {
                    logger.warn("Invalid line format (expected key-value pair): {}", line);
                }
            }
        }
        
        return keyValues;
    }
}