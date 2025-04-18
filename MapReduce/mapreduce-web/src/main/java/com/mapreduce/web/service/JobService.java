package com.mapreduce.web.service;

import com.mapreduce.web.dao.JobDAO;
import com.mapreduce.web.dao.WordCountDAO;
import com.mapreduce.web.model.Job;
import com.mapreduce.web.model.WordCount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Service for job-related operations
 */
public class JobService {
    //private static final Logger logger = LogManager.getLogger(JobService.class);
    
    /**
     * Submit a new job
     */
    public static String submitJob(String inputText, String fileName, int numReduceTasks, boolean useBlob) 
            throws IOException, SQLException, TimeoutException {
        // Validate input parameters
        if (inputText == null || inputText.trim().isEmpty()) {
            throw new IllegalArgumentException("Input text cannot be empty");
        }
        
        if (fileName == null || fileName.trim().isEmpty()) {
            fileName = "input.txt";
        }
        
        if (numReduceTasks <= 0) {
            numReduceTasks = 5; // Default value
        }
        
        BlobStorageService blobService = new BlobStorageService();
    
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(formatter);
        // Generate jobId
        String jobId = "job_" + formattedDateTime;
        
        // Create blob name
        String blobName = "input/" + jobId + "/" + fileName;
        
        try {
            // Upload text to Blob Storage
            String blobUrl = blobService.saveTextToBlob(inputText, blobName);
            //logger.info("Uploaded text to Blob: {} -> {}", blobName, blobUrl);
            
            // Determine storage type
            Job.StorageType storageType = useBlob ? Job.StorageType.BLOB : Job.StorageType.LOCAL;
            
            // Create job record in database
            JobDAO.createJob(jobId, fileName, numReduceTasks, storageType, blobUrl);
            //logger.info("Created job record in database: {}", jobId);
            
            // Send message to RabbitMQ
            MessageService.sendJobRequest(jobId, fileName, blobUrl, numReduceTasks);
            //logger.info("Sent job request message to queue: {}", jobId);
            
            return jobId;
        } catch (Exception e) {
            //logger.error("Error submitting job", e);
            throw e;
        }
    }
    
    /**
     * Get job status and information
     */
    public static Job getJobStatus(String jobId) throws SQLException {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("Job ID cannot be empty");
        }
        
        return JobDAO.getJob(jobId);
    }
    
    /**
     * Get all jobs
     */
    public static List<Job> getAllJobs() throws SQLException {
        return JobDAO.getAllJobs();
    }
    
    /**
     * Get word count results for a job
     */
    public static List<WordCount> getWordCounts(String jobId, int limit) throws SQLException {
        // Verify job exists and is completed
        Job job = JobDAO.getJob(jobId);
        
        if (job == null) {
            throw new IllegalArgumentException("Job not found: " + jobId);
        }
        
        if (job.getStatus() != Job.Status.COMPLETED) {
            throw new IllegalStateException("Job is not completed yet");
        }
        
        // Get word counts from database
        return WordCountDAO.getWordCounts(jobId, limit);
    }
    
    /**
     * Get total word frequency for a job
     */
    public static int getTotalWordFrequency(String jobId) throws SQLException {
        return WordCountDAO.getTotalWordFrequency(jobId);
    }
    
    /**
     * Get total unique word count for a job
     */
    public static int getUniqueWordCount(String jobId) throws SQLException {
        return WordCountDAO.getWordCountTotal(jobId);
    }
}