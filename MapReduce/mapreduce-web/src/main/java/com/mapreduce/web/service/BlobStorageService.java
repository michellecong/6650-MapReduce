package com.mapreduce.web.service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.mapreduce.web.config.AppConfig;

/**
 * Azure Blob Storage Service for file operations
 */
public class BlobStorageService {
    private static final Logger logger = LogManager.getLogger(BlobStorageService.class);
    
    private final BlobServiceClient blobServiceClient;
    private final BlobContainerClient containerClient;
    
    /**
     * Constructor - initializes Azure Blob Storage client
     */
    public BlobStorageService() {
        String connectionString = AppConfig.AZURE_CONNECTION_STRING;
        String containerName = AppConfig.AZURE_CONTAINER_NAME;
        
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalStateException("Azure Storage connection string not configured");
        }
        
        // Create Azure Blob Storage client
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
        
        // Get or create container
        this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!containerClient.exists()) {
            containerClient.create();
            logger.info("Created Azure Blob container: {}", containerName);
        }
        
        logger.info("Blob storage service initialized successfully");
    }
    
    /**
     * Upload text content to Blob Storage
     */
    public String uploadText(String content, String blobName) throws IOException {
        logger.info("Uploading text content to Blob: {}", blobName);
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            InputStream dataStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
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
    
    /**
     * Upload file to Blob Storage
     */
    public String uploadFile(String filePath, String blobName) throws IOException {
        logger.info("Uploading file to Blob storage: {} -> {}", filePath, blobName);
        
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("File not found: " + filePath);
        }
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.uploadFromFile(filePath, true);
            
            String blobUrl = blobClient.getBlobUrl();
            logger.info("File uploaded successfully: {}", blobUrl);
            return blobUrl;
        } catch (Exception e) {
            logger.error("Failed to upload file to Blob storage", e);
            throw new IOException("Failed to upload file to Blob storage", e);
        }
    }
    
    /**
     * Save text content to temporary file and upload to Blob Storage
     */
    public String saveTextToBlob(String content, String blobName) throws IOException {
        // Create temporary file
        File tempFile = File.createTempFile("mapreduce-", ".txt");
        
        try {
            // Write content to file
            FileUtils.writeStringToFile(tempFile, content, StandardCharsets.UTF_8);
            
            // Upload file to Blob
            return uploadFile(tempFile.getAbsolutePath(), blobName);
        } finally {
            // Clean up temporary file
            try {
                Files.deleteIfExists(tempFile.toPath());
            } catch (IOException e) {
                logger.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath(), e);
            }
        }
    }
    
    /**
     * Check if blob exists
     */
    public boolean blobExists(String blobName) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        return blobClient.exists();
    }
    
    /**
     * Extract blob name from URL
     */
    public static String getBlobNameFromUrl(String blobUrl) {
        if (blobUrl == null || blobUrl.isEmpty()) {
            return null;
        }
        
        int lastSlashIndex = blobUrl.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < blobUrl.length() - 1) {
            return blobUrl.substring(lastSlashIndex + 1);
        }
        
        return blobUrl;
    }
    
    /**
     * Download blob content as text
     */
    public String downloadBlobAsText(String blobName) throws IOException {
        logger.info("Downloading blob as text: {}", blobName);
        
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            if (!blobClient.exists()) {
                throw new IOException("Blob not found: " + blobName);
            }
            
            File tempFile = File.createTempFile("download-", ".txt");
            blobClient.downloadToFile(tempFile.getAbsolutePath(), true);
            
            String content = FileUtils.readFileToString(tempFile, StandardCharsets.UTF_8);
            Files.deleteIfExists(tempFile.toPath());
            
            return content;
        } catch (Exception e) {
            logger.error("Failed to download blob content", e);
            throw new IOException("Failed to download blob content: " + blobName, e);
        }
    }
}