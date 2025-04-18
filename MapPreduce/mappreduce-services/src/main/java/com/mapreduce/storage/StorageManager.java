package com.mapreduce.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.StorageType;
import com.mapreduce.db.WordCountDao;

/**
 * 存储管理器 - 完全使用Blob存储
 */
public class StorageManager {
    private static final Logger logger = LogManager.getLogger(StorageManager.class);
    
    private final String baseDir;
    private final BlobStorageService blobStorageService;
    private boolean blobEnabled = false;
    
    /**
     * 创建存储管理器
     */
    public StorageManager(String baseDir) {
        this.baseDir = baseDir;
        this.blobStorageService = new BlobStorageService();
        
        try {
            this.blobEnabled = ConfigManager.isAzureStorageEnabled();
            logger.info("StorageManager initialized with blob storage: {}", blobEnabled);
        } catch (Exception e) {
            logger.error("Failed to check if blob storage is enabled", e);
        }
    }
    
    /**
     * 初始化存储系统
     */
    public void initialize() throws IOException {
        // 创建必要的本地临时目录（仅用于临时文件）
        Files.createDirectories(Paths.get(getTempDirectory()));
        
        // 保留创建这些目录以便兼容现有代码
        Files.createDirectories(Paths.get(getInputDirectory()));
        Files.createDirectories(Paths.get(getIntermediateDirectory()));
        Files.createDirectories(Paths.get(getOutputDirectory()));
        
        logger.info("Storage manager initialized with base directory: {}, using Blob storage for all data", baseDir);
    }
    
    /**
     * 获取Blob存储服务
     */
    public BlobStorageService getBlobStorageService() {
        return blobStorageService;
    }
    
    /**
     * 获取临时目录
     */
    public String getTempDirectory() {
        return baseDir + "/temp";
    }
    
    /**
     * 获取输入目录
     */
    public String getInputDirectory() {
        return baseDir + "/" + ConfigManager.INPUT_DIR;
    }
    
    /**
     * 获取中间目录
     */
    public String getIntermediateDirectory() {
        return baseDir + "/" + ConfigManager.INTERMEDIATE_DIR;
    }
    
    /**
     * 获取输出目录
     */
    public String getOutputDirectory() {
        return baseDir + "/" + ConfigManager.OUTPUT_DIR;
    }
    
    /**
     * 获取作业的输入目录
     */
    public String getInputDirectory(String jobId) {
        return getInputDirectory() + "/" + jobId;
    }
    
    /**
     * 获取作业的中间目录
     */
    public String getIntermediateDirectory(String jobId) {
        return getIntermediateDirectory() + "/" + jobId;
    }
    
    /**
     * 获取作业的输出目录
     */
    public String getOutputDirectory(String jobId) {
        return getOutputDirectory() + "/" + jobId;
    }
    
    /**
     * 获取Blob路径名称 - 输入
     */
    public String getInputBlobPath(String jobId) {
        return "input/" + jobId;
    }
    
    /**
     * 获取Blob路径名称 - 中间结果
     */
    public String getIntermediateBlobPath(String jobId) {
        return "intermediate/" + jobId;
    }
    
    /**
     * 获取Blob路径名称 - 输出
     */
    public String getOutputBlobPath(String jobId) {
        return "output/" + jobId;
    }
    
    /**
     * 上传输入文件到Blob存储
     * @param inputPath 本地输入文件路径
     * @param jobId 作业ID
     * @return Blob URL
     */
    public String uploadInputToBlob(String inputPath, String jobId) throws IOException {
        if (!blobEnabled || !blobStorageService.isEnabled()) {
            throw new IOException("Blob storage is not enabled but required for operation");
        }
        
        String blobName = getInputBlobPath(jobId) + "/" + new File(inputPath).getName();
        return blobStorageService.uploadFile(inputPath, blobName);
    }
    
    /**
     * 从Blob下载文件到临时目录
     * @param blobUrl Blob URL
     * @return 临时文件路径
     */
    public String downloadBlobToTemp(String blobUrl) throws IOException {
        if (!blobEnabled || !blobStorageService.isEnabled() || blobUrl == null || blobUrl.isEmpty()) {
            throw new IOException("Blob storage is not enabled or URL is invalid");
        }
        
        String tempFileName = "temp_" + System.currentTimeMillis() + "_" + 
                             Math.abs(blobUrl.hashCode()) % 10000 + ".txt";
        String tempFilePath = getTempDirectory() + "/" + tempFileName;
        
        try {
            // 确保目录存在
            Files.createDirectories(Paths.get(getTempDirectory()));
            
            // 获取Blob名称
            String blobName = blobStorageService.getBlobNameFromUrl(blobUrl);
            if (blobName == null) {
                throw new IOException("Failed to extract blob name from URL: " + blobUrl);
            }
            
            // 下载文件
            logger.info("Downloading from Blob to temp file: {} -> {}", blobUrl, tempFilePath);
            blobStorageService.downloadFile(blobName, tempFilePath);
            
            return tempFilePath;
        } catch (Exception e) {
            logger.error("Failed to download from Blob: {}", blobUrl, e);
            throw new IOException("Failed to download from Blob: " + blobUrl, e);
        }
    }
    
    /**
     * 从 Reduce 输出文件加载词频统计结果到数据库
     */
    public void loadReduceOutputToDatabase(String jobId, String outputFile, StorageType storageType, String blobUrl) throws IOException, SQLException {
        // 强制使用Blob存储，忽略本地文件路径
        if (blobUrl == null || blobUrl.isEmpty()) {
            logger.warn("No blob URL provided for reduce output, cannot load to database");
            return;
        }
        
        logger.info("Loading reduce output from Blob to database: {}", blobUrl);
        
        Map<String, Integer> wordCounts = new HashMap<>();
        String tempFile = null;
        
        try {
            // 下载Blob到临时文件
            tempFile = downloadBlobToTemp(blobUrl);
            File file = new File(tempFile);
            
            if (!file.exists() || file.length() == 0) {
                logger.warn("Downloaded Blob file is empty or does not exist: {}", tempFile);
                return; // 删除测试词条件，如果文件为空则不添加任何内容
            } else {
                // 读取词频
                readWordCountsFromFile(tempFile, wordCounts);
            }
            
            // 打印调试信息
            logger.info("Read {} unique words from Blob file", wordCounts.size());
            
            // 如果读取到词频，保存到数据库
            if (!wordCounts.isEmpty()) {
                try {
                    WordCountDao.saveWordCounts(jobId, wordCounts);
                    logger.info("Successfully saved {} word counts to database for job {}", wordCounts.size(), jobId);
                } catch (Exception e) {
                    logger.error("Failed to save word counts to database: {}", e.getMessage(), e);
                }
            } else {
                logger.warn("No word counts found in Blob file");
            }
        } catch (Exception e) {
            logger.error("Error loading reduce output to database from Blob: {}", e.getMessage(), e);
            throw e;
        } finally {
            // 清理临时文件
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(Paths.get(tempFile));
                } catch (IOException e) {
                    logger.warn("Failed to delete temp file: {}", tempFile);
                }
            }
        }
    }
    /**
     * 从文件读取词频统计
     */
    private void readWordCountsFromFile(String filePath, Map<String, Integer> wordCounts) throws IOException {
        logger.info("Reading word counts from file: {}", filePath);
        int lineCount = 0;
        int errorCount = 0;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineCount++;
                
                // 跳过空行
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                // 解析行
                String[] parts = line.split("\\t");
                if (parts.length == 2) {
                    try {
                        String word = parts[0].trim();
                        int count = Integer.parseInt(parts[1].trim());
                        
                        if (!word.isEmpty() && count > 0) {
                            wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                            
                            // 打印前20个词以便调试
                            if (wordCounts.size() <= 20) {
                                logger.debug("Word count: {} -> {}", word, count);
                            }
                        }
                    } catch (NumberFormatException e) {
                        errorCount++;
                        logger.warn("Invalid format in line {}: {}", lineCount, line);
                    }
                } else {
                    errorCount++;
                    logger.warn("Invalid line format at line {}: {}", lineCount, line);
                }
            }
        }
        
        logger.info("Finished reading word counts: {} lines processed, {} errors, {} unique words found",
                  lineCount, errorCount, wordCounts.size());
    }
    
    /**
     * 从Blob下载文件到本地
     * @param blobUrl Blob URL
     * @param localFilePath 本地文件路径
     * @return 是否下载成功
     */
    public boolean downloadFromBlob(String blobUrl, String localFilePath) {
        if (!blobEnabled || !blobStorageService.isEnabled() || blobUrl == null || blobUrl.isEmpty()) {
            return false;
        }
        
        try {
            String blobName = blobStorageService.getBlobNameFromUrl(blobUrl);
            if (blobName != null) {
                Files.createDirectories(Paths.get(localFilePath).getParent());
                blobStorageService.downloadFile(blobName, localFilePath);
                return true;
            }
        } catch (Exception e) {
            logger.error("Failed to download from blob: {}", blobUrl, e);
        }
        
        return false;
    }
    
    /**
     * 合并所有输出文件到一个最终结果
     */
    public String mergeOutputFiles(String jobId, String outputFileName, StorageType storageType) throws IOException, SQLException {
        logger.info("Merging output files for job: {} using database-stored word counts", jobId);
        
        // 直接从数据库中获取合并的结果
        Map<String, Integer> finalWordCounts = WordCountDao.getWordCounts(jobId);
        
        if (finalWordCounts.isEmpty()) {
            logger.warn("No word counts found in database for job {}", jobId);
            return null;
        }
        
        // 将结果转换为字符串
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, Integer> entry : finalWordCounts.entrySet()) {
            content.append(entry.getKey()).append("\t").append(entry.getValue()).append(System.lineSeparator());
        }
        
        // 上传最终结果到Blob
        String blobName = getOutputBlobPath(jobId) + "/final_result.txt";
        String blobUrl = blobStorageService.uploadText(content.toString(), blobName);
        
        logger.info("Merged output uploaded to Blob: {}", blobUrl);
        return blobUrl;
    }
    
    /**
     * 清理指定作业的所有Blob数据
     */
    public void cleanup(String jobId) {
        logger.info("Cleaning up Blob storage for job: {}", jobId);
        
        // 由于不再使用本地文件，这里只需要清理Blob临时目录
        // 注意：最终结果不应该被删除
        
        if (blobEnabled && blobStorageService.isEnabled()) {
            // 这里实现一个简化版的清理逻辑
            // 在实际生产环境中，你可能需要更复杂的逻辑来枚举和删除Blob
            logger.info("Note: Temporary Blobs should be cleaned up periodically to save storage costs");
        }
        
        // 清理本地临时目录
        try {
            File tempDir = new File(getTempDirectory() + "/" + jobId);
            if (tempDir.exists()) {
                deleteDirectorySafely(tempDir);
            }
        } catch (Exception e) {
            logger.warn("Error cleaning up temporary directory: {}", e.getMessage());
        }
    }
    
    /**
     * 安全地递归删除目录
     */
    private void deleteDirectorySafely(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectorySafely(file);
                    } else {
                        // 尝试删除文件，忽略错误
                        try {
                            boolean deleted = file.delete();
                            if (!deleted) {
                                logger.warn("Failed to delete file: {}", file.getAbsolutePath());
                                // 尝试在JVM退出时删除
                                file.deleteOnExit();
                            }
                        } catch (Exception e) {
                            logger.warn("Error deleting file {}: {}", file.getAbsolutePath(), e.getMessage());
                            // 尝试在JVM退出时删除
                            file.deleteOnExit();
                        }
                    }
                }
            }
            
            // 尝试删除目录本身
            try {
                boolean deleted = directory.delete();
                if (!deleted) {
                    logger.warn("Failed to delete directory: {}", directory.getAbsolutePath());
                    // 尝试在JVM退出时删除
                    directory.deleteOnExit();
                }
            } catch (Exception e) {
                logger.warn("Error deleting directory {}: {}", directory.getAbsolutePath(), e.getMessage());
                // 尝试在JVM退出时删除
                directory.deleteOnExit();
            }
        }
    }
}