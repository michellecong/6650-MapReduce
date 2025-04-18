package com.mapreduce.master;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.db.JobDao;
import com.mapreduce.storage.BlobStorageService;
import com.mapreduce.storage.StorageManager;

/**
 * 文件分区处理器
 */
public class FilePartitioner {
    private static final Logger logger = LogManager.getLogger(FilePartitioner.class);
    
    private final TaskScheduler taskScheduler;
    private final String baseDir;
    private final StorageManager storageManager;
    private final BlobStorageService blobStorageService;
    
    /**
     * 创建文件分区处理器
     */
    public FilePartitioner(TaskScheduler taskScheduler, String baseDir, 
                          StorageManager storageManager, BlobStorageService blobStorageService) {
        this.taskScheduler = taskScheduler;
        this.baseDir = baseDir;
        this.storageManager = storageManager;
        this.blobStorageService = blobStorageService;
    }
    
    /**
     * 分割文件并创建 Map 任务
     * 
     * @param inputPath 输入文件路径
     * @param jobId 作业ID
     * @param storageType 存储类型
     * @param inputBlobUrl 输入Blob URL
     * @return 创建的 Map 任务列表
     */
    public List<Task> splitFileAndCreateMapTasks(String inputPath, String jobId, 
                                               StorageType storageType, String inputBlobUrl) 
            throws IOException, SQLException {
            logger.info("Splitting file: {} for job: {}, storageType: {}", inputPath, jobId, storageType);
            
            // 创建作业输入和中间目录
            String jobInputDir = baseDir + "/" + ConfigManager.INPUT_DIR + "/" + jobId;
            String jobIntermediateDir = baseDir + "/" + ConfigManager.INTERMEDIATE_DIR + "/" + jobId;
            Files.createDirectories(Paths.get(jobInputDir));
            Files.createDirectories(Paths.get(jobIntermediateDir));
            
            // 如果是Blob存储，可能需要先下载文件
            if (storageType == StorageType.BLOB && inputBlobUrl != null && !inputBlobUrl.isEmpty() && blobStorageService != null) {
                String tempInputPath = jobInputDir + "/input_original.txt";
                try {
                    logger.info("Downloading input file from Blob: {} -> {}", inputBlobUrl, tempInputPath);
                    blobStorageService.downloadFile(blobStorageService.getBlobNameFromUrl(inputBlobUrl), tempInputPath);
                    
                    // 使用下载的临时文件
                    inputPath = tempInputPath;
                } catch (Exception e) {
                    logger.error("Failed to download input file from Blob", e);
                    // 继续使用原始文件路径，尝试本地访问
                }
            }
            
            File inputFile = new File(inputPath);
            if (!inputFile.exists()) {
                throw new IOException("Input file not found: " + inputPath);
            }
            
            long fileSize = inputFile.length();
            int maxChunkSize = ConfigManager.getDefaultChunkSize();
            int numChunks = (int) Math.ceil((double) fileSize / maxChunkSize);
            
            List<Task> mapTasks = new ArrayList<>();
            
            if (numChunks <= 1) {
                // 文件太小，不需要分割
                String chunkPath = jobInputDir + "/chunk_0.txt";
                FileUtils.copyFile(inputFile, new File(chunkPath));
                
                String outputPath = jobIntermediateDir + "/map_0";
                
                // 根据存储类型创建任务
                Task mapTask;
                if (storageType == StorageType.BLOB) {
                    // 如果是Blob存储，上传分块到Blob
                    String chunkBlobUrl = null;
                    if (blobStorageService != null) {
                        String blobName = "input/" + jobId + "/chunk_0.txt";
                        chunkBlobUrl = blobStorageService.uploadFile(chunkPath, blobName);
                    }
                    
                    mapTask = Task.createBlobMapTask(jobId, chunkPath, outputPath, chunkBlobUrl);
                } else {
                    mapTask = Task.createMapTask(jobId, chunkPath, outputPath);
                }
                
                mapTasks.add(mapTask);
            } else {
                // 按行分割文件
                int chunkIndex = 0;
                StringBuilder chunkContent = new StringBuilder();
                long currentSize = 0;
                
                try (LineIterator it = FileUtils.lineIterator(inputFile, StandardCharsets.UTF_8.name())) {
                    while (it.hasNext()) {
                        String line = it.nextLine();
                        int lineSize = line.length() + 1; // +1 表示换行符
                        
                        if (currentSize + lineSize > maxChunkSize && currentSize > 0) {
                            // 当前块已满，写入文件
                            String chunkPath = jobInputDir + "/chunk_" + chunkIndex + ".txt";
                            FileUtils.writeStringToFile(new File(chunkPath), chunkContent.toString(), StandardCharsets.UTF_8);
                            
                            // 创建 Map 任务
                            String outputPath = jobIntermediateDir + "/map_" + chunkIndex;
                            
                            // 根据存储类型创建任务
                            Task mapTask;
                            if (storageType == StorageType.BLOB) {
                                // 如果是Blob存储，上传分块到Blob
                                String chunkBlobUrl = null;
                                if (blobStorageService != null) {
                                    String blobName = "input/" + jobId + "/chunk_" + chunkIndex + ".txt";
                                    chunkBlobUrl = blobStorageService.uploadFile(chunkPath, blobName);
                                }
                                
                                mapTask = Task.createBlobMapTask(jobId, chunkPath, outputPath, chunkBlobUrl);
                            } else {
                                mapTask = Task.createMapTask(jobId, chunkPath, outputPath);
                            }
                            
                            mapTasks.add(mapTask);
                            
                            // 重置块
                            chunkContent = new StringBuilder();
                            currentSize = 0;
                            chunkIndex++;
                        }
                        
                        chunkContent.append(line).append(System.lineSeparator());
                        currentSize += lineSize;
                    }
                    
                    // 写入最后一个块
                    if (currentSize > 0) {
                        String chunkPath = jobInputDir + "/chunk_" + chunkIndex + ".txt";
                        FileUtils.writeStringToFile(new File(chunkPath), chunkContent.toString(), StandardCharsets.UTF_8);
                        
                        // 创建 Map 任务
                        String outputPath = jobIntermediateDir + "/map_" + chunkIndex;
                        
                        // 根据存储类型创建任务
                        Task mapTask;
                        if (storageType == StorageType.BLOB) {
                            // 如果是Blob存储，上传分块到Blob
                            String chunkBlobUrl = null;
                            if (blobStorageService != null) {
                                String blobName = "input/" + jobId + "/chunk_" + chunkIndex + ".txt";
                                chunkBlobUrl = blobStorageService.uploadFile(chunkPath, blobName);
                            }
                            
                            mapTask = Task.createBlobMapTask(jobId, chunkPath, outputPath, chunkBlobUrl);
                        } else {
                            mapTask = Task.createMapTask(jobId, chunkPath, outputPath);
                        }
                        
                        mapTasks.add(mapTask);
                    }
                }
            }
            
            // 将任务添加到调度器
            taskScheduler.addTasks(mapTasks);
            
            // 更新作业的 Map 任务数量
            JobDao.updateMapTaskCount(jobId, mapTasks.size());
            
            logger.info("Created {} map tasks for job: {}", mapTasks.size(), jobId);
            return mapTasks;
        }
    }