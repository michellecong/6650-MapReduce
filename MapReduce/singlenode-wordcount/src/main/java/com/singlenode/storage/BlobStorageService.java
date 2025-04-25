package com.singlenode.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.singlenode.util.ConfigManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Blob存储服务，用于上传和下载文件
 */
public class BlobStorageService {
  private static final Logger logger = LogManager.getLogger(BlobStorageService.class);

  private final BlobServiceClient blobServiceClient;
  private final BlobContainerClient containerClient;

  /**
   * 创建Blob存储服务
   */
  public BlobStorageService() {
    String connectionString = ConfigManager.getProperty("storage.connection.string",
        "DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=yourkey;EndpointSuffix=core.windows.net");
    String containerName = ConfigManager.getProperty("storage.container.name", "singlenode");

    // 创建BlobServiceClient
    blobServiceClient = new BlobServiceClientBuilder()
        .connectionString(connectionString)
        .buildClient();

    // 获取容器（如果不存在则创建）
    containerClient = blobServiceClient.getBlobContainerClient(containerName);
    if (!containerClient.exists()) {
      containerClient.create();
      logger.info("Created blob container: {}", containerName);
    }
  }

  /**
   * 上传文件到Blob存储
   */
  public String uploadFile(File file, String blobName) throws IOException {
    logger.info("Uploading file to blob storage: {}", blobName);
    BlobClient blobClient = containerClient.getBlobClient(blobName);

    try {
      blobClient.uploadFromFile(file.getAbsolutePath(), true);
      String blobUrl = blobClient.getBlobUrl();
      logger.info("File uploaded successfully: {}", blobUrl);
      return blobUrl;
    } catch (Exception e) {
      logger.error("Error uploading file to blob storage", e);
      throw new IOException("Error uploading file to blob storage", e);
    }
  }

  /**
   * 上传文本内容到Blob存储
   */
  public String uploadText(String content, String blobName) throws IOException {
    logger.info("Uploading text content to blob storage: {}", blobName);
    BlobClient blobClient = containerClient.getBlobClient(blobName);

    try (InputStream dataStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
      blobClient.upload(dataStream, content.length(), true);
      String blobUrl = blobClient.getBlobUrl();
      logger.info("Text content uploaded successfully: {}", blobUrl);
      return blobUrl;
    } catch (Exception e) {
      logger.error("Error uploading text content to blob storage", e);
      throw new IOException("Error uploading text content to blob storage", e);
    }
  }

  /**
   * 从Blob存储下载文件
   */
  public void downloadFile(String blobUrl, String destinationPath) throws IOException {
    logger.info("Downloading file from blob storage: {}", blobUrl);

    try {
      // 从URL中提取blobName
      String blobName = extractBlobNameFromUrl(blobUrl);
      BlobClient blobClient = containerClient.getBlobClient(blobName);

      // 创建目标目录（如果不存在）
      Path destPath = Paths.get(destinationPath);
      Files.createDirectories(destPath.getParent());

      // 下载文件
      try (FileOutputStream outputStream = new FileOutputStream(destinationPath)) {
        blobClient.downloadStream(outputStream);
      }

      logger.info("File downloaded successfully to: {}", destinationPath);
    } catch (Exception e) {
      logger.error("Error downloading file from blob storage", e);
      throw new IOException("Error downloading file from blob storage", e);
    }
  }

  /**
   * 从URL提取Blob名称
   */
  private String extractBlobNameFromUrl(String blobUrl) {
    // 简单实现：从URL中提取最后一部分作为blobName
    // 实际项目中可能需要更复杂的解析逻辑
    int lastSlashIndex = blobUrl.lastIndexOf('/');
    if (lastSlashIndex == -1 || lastSlashIndex == blobUrl.length() - 1) {
      throw new IllegalArgumentException("Invalid blob URL: " + blobUrl);
    }
    return blobUrl.substring(lastSlashIndex + 1);
  }
}