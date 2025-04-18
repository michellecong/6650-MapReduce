package com.mapreduce.storage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.mapreduce.common.ConfigManager;

/**
 * Blob Storage Service, encapsulates Azure Blob Storage operations
 */
public class BlobStorageService {
    private static final Logger logger = LogManager.getLogger(BlobStorageService.class);
    
    private BlobServiceClient blobServiceClient;
    private BlobContainerClient containerClient;
    private boolean isEnabled = false;
    
    /**
     * Create Blob Storage Service
     */
    public BlobStorageService() {
        try {
            initialize();
        } catch (Exception e) {
            logger.error("Failed to initialize BlobStorageService", e);
        }
    }
    
    /**
     * Initialize Blob Storage client
     */
    public void initialize() throws Exception {
        // Read from config manager
        String connectionString = ConfigManager.getAzureConnectionString();
        String containerName = ConfigManager.getAzureContainerName();
        isEnabled = ConfigManager.isAzureStorageEnabled();
        
        if (!isEnabled) {
            logger.info("Blob storage is disabled in configuration");
            return;
        }
        
        if (connectionString == null || connectionString.isEmpty()) {
            logger.warn("Azure Storage connection string is not configured");
            isEnabled = false;
            return;
        }
        
        try {
            // Create Azure Blob storage client
            blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .buildClient();
            
            // Get or create container
            containerClient = blobServiceClient.getBlobContainerClient(containerName);
            if (!containerClient.exists()) {
                containerClient.create();
                logger.info("Created Azure Blob container: {}", containerName);
            }
            
            logger.info("Blob storage service initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize Azure Blob storage client", e);
            isEnabled = false;
            throw e;
        }
    }
    
    /**
     * Refresh configuration
     */
    public void refreshConfiguration() throws Exception {
        // Read from config manager
        String connectionString = ConfigManager.getAzureConnectionString();
        String containerName = ConfigManager.getAzureContainerName();
        isEnabled = ConfigManager.isAzureStorageEnabled();
        
        if (isEnabled && blobServiceClient == null) {
            initialize();
        }
    }
    
    /**
     * Upload file to Blob storage
     * @param localFilePath local file path
     * @param blobName Blob name
     * @return Blob URL
     */
    public String uploadFile(String localFilePath, String blobName) throws IOException {
        if (!isEnabled) {
            logger.debug("Blob storage is disabled, not uploading file: {}", localFilePath);
            return null;
        }
        
        logger.info("Uploading file to Blob storage: {} -> {}", localFilePath, blobName);
        
        File file = new File(localFilePath);
        if (!file.exists()) {
            throw new IOException("File not found: " + localFilePath);
        }
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.uploadFromFile(localFilePath, true);
            
            String blobUrl = blobClient.getBlobUrl();
            logger.info("File uploaded successfully: {}", blobUrl);
            return blobUrl;
        } catch (Exception e) {
            logger.error("Failed to upload file to Blob storage", e);
            throw new IOException("Failed to upload file to Blob storage", e);
        }
    }
    
    /**
     * Download Blob to local file
     * @param blobName Blob name
     * @param localFilePath local file path
     */
    public void downloadFile(String blobName, String localFilePath) throws IOException {
        if (!isEnabled) {
            logger.debug("Blob storage is disabled, not downloading: {}", blobName);
            return;
        }
        
        logger.info("Downloading from Blob storage: {} -> {}", blobName, localFilePath);
        
        try {
            // Ensure target directory exists
            Path localPath = Paths.get(localFilePath);
            Files.createDirectories(localPath.getParent());
            
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.downloadToFile(localFilePath, true);
            
            logger.info("File downloaded successfully from Blob storage");
        } catch (Exception e) {
            logger.error("Failed to download file from Blob storage", e);
            throw new IOException("Failed to download file from Blob storage", e);
        }
    }
    
    /**
     * Extract blobName from URL
     * @param blobUrl Blob URL
     * @return Blob name
     */
    // public String getBlobNameFromUrl(String blobUrl) {
    //     if (blobUrl == null || blobUrl.isEmpty()) {
    //         return null;
    //     }
        
    //     // Extract blob name from URL
    //     int lastSlashIndex = blobUrl.lastIndexOf('/');
    //     if (lastSlashIndex >= 0 && lastSlashIndex < blobUrl.length() - 1) {
    //         return blobUrl.substring(lastSlashIndex + 1);
    //     }
        
    //     return blobUrl;
    // }
    /**
     * Extract blobName from URL
     * @param blobUrl Blob URL
     * @return Blob name
     */
    public String getBlobNameFromUrl(String blobUrl) {
        if (blobUrl == null || blobUrl.isEmpty()) {
            return null;
        }
        
        logger.debug("Extracting blob name from URL: {}", blobUrl);
        
        try {
            // 移除查询参数
            int queryIndex = blobUrl.indexOf('?');
            if (queryIndex > 0) {
                blobUrl = blobUrl.substring(0, queryIndex);
            }
            
            // 处理URL编码
            java.net.URL url = new java.net.URL(blobUrl);
            String path = url.getPath();
            
            // 移除容器名称前缀
            String containerName = "/" + ConfigManager.getAzureContainerName() + "/";
            if (path.startsWith(containerName)) {
                path = path.substring(containerName.length());
            }
            
            // 处理URL编码的路径
            path = java.net.URLDecoder.decode(path, StandardCharsets.UTF_8.name());
            
            logger.debug("Extracted blob name: {}", path);
            return path;
        } catch (Exception e) {
            logger.error("Failed to extract blob name from URL: {}", blobUrl, e);
            
            // 降级为简单提取
            try {
                int containerStart = blobUrl.indexOf(ConfigManager.getAzureContainerName());
                if (containerStart >= 0) {
                    int pathStart = blobUrl.indexOf('/', containerStart + ConfigManager.getAzureContainerName().length() + 1);
                    if (pathStart >= 0) {
                        String path = blobUrl.substring(pathStart + 1);
                        path = java.net.URLDecoder.decode(path, StandardCharsets.UTF_8.name());
                        logger.debug("Fallback extracted blob name: {}", path);
                        return path;
                    }
                }
            } catch (Exception ex) {
                logger.error("Fallback extraction also failed", ex);
            }
        }
        
        // 最后尝试简单提取
        int lastSlashIndex = blobUrl.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < blobUrl.length() - 1) {
            String fileName = blobUrl.substring(lastSlashIndex + 1);
            logger.warn("Using simple extraction, only got filename: {}", fileName);
            return fileName;
        }
        
        return blobUrl;
    }
    /**
     * Check if Blob exists
     * @param blobName Blob name
     * @return whether it exists
     */
    public boolean blobExists(String blobName) {
        if (!isEnabled || blobName == null || blobName.isEmpty()) {
            return false;
        }
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            return blobClient.exists();
        } catch (Exception e) {
            logger.error("Failed to check blob existence: {}", blobName, e);
            return false;
        }
    }
    
    /**
     * Get Blob size
     * @param blobName Blob name
     * @return Blob size (bytes)
     */
    public long getBlobSize(String blobName) {
        if (!isEnabled || blobName == null || blobName.isEmpty()) {
            return -1;
        }
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            if (!blobClient.exists()) {
                return -1;
            }
            
            BlobProperties properties = blobClient.getProperties();
            return properties.getBlobSize();
        } catch (Exception e) {
            logger.error("Failed to get blob size: {}", blobName, e);
            return -1;
        }
    }
    
    /**
     * Delete Blob
     * @param blobName Blob name
     * @return whether deletion was successful
     */
    public boolean deleteBlob(String blobName) {
        if (!isEnabled || blobName == null || blobName.isEmpty()) {
            return false;
        }
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            return blobClient.deleteIfExists();
        } catch (Exception e) {
            logger.error("Failed to delete blob: {}", blobName, e);
            return false;
        }
    }
    
    /**
     * Delete Blob by URL
     * @param blobUrl Blob URL
     * @return whether deletion was successful
     */
    public boolean deleteBlobByUrl(String blobUrl) {
        String blobName = getBlobNameFromUrl(blobUrl);
        return blobName != null && deleteBlob(blobName);
    }
    
    /**
     * Check if Blob Storage is enabled
     */
    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * Upload text content to Blob
     * @param content text content
     * @param blobName Blob name
     * @return Blob URL
     */
    public String uploadText(String content, String blobName) throws IOException {
        if (!isEnabled) {
            logger.debug("Blob storage is disabled, not uploading text content");
            return null;
        }
        
        logger.info("Uploading text content to Blob storage: {}", blobName);
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            InputStream dataStream = new ByteArrayInputStream(content.getBytes());
            blobClient.upload(dataStream, content.length(), true);
            dataStream.close();
            
            String blobUrl = blobClient.getBlobUrl();
            logger.info("Text content uploaded successfully: {}", blobUrl);
            return blobUrl;
        } catch (Exception e) {
            logger.error("Failed to upload text content to Blob storage", e);
            throw new IOException("Failed to upload text content to Blob storage", e);
        }
    }
}