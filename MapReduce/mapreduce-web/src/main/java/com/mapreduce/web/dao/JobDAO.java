package com.mapreduce.web.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.web.model.Job;
import com.mapreduce.web.util.DatabaseUtil;

/**
 * Data Access Object for Job-related operations
 */
public class JobDAO {
    private static final Logger logger = LogManager.getLogger(JobDAO.class);
    
    /**
     * Create a new job in the database
     */
    public static String createJob(String jobId, String inputFile, int numReduceTasks, Job.StorageType storageType, String inputBlobUrl) throws SQLException {

        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "INSERT INTO jobs (job_id, input_file, num_reduce_tasks, status, storage_type, input_blob_url) " + 
                         "VALUES (?, ?, ?, ?, ?, ?)";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            stmt.setString(2, inputFile);
            stmt.setInt(3, numReduceTasks);
            stmt.setString(4, Job.Status.PENDING.name());
            stmt.setString(5, storageType.name());
            stmt.setString(6, inputBlobUrl);
            
            stmt.executeUpdate();
            
            logger.info("Created job: {} with storage type: {}", jobId, storageType);
            return jobId;
        } finally {
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Get job by ID
     */
    public static Job getJob(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT * FROM jobs WHERE job_id = ?";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, jobId);
            
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                Job job = new Job();
                job.setJobId(rs.getString("job_id"));
                job.setStatus(Job.Status.valueOf(rs.getString("status")));
                job.setInputFile(rs.getString("input_file"));
                job.setNumMapTasks(rs.getInt("num_map_tasks"));
                job.setNumReduceTasks(rs.getInt("num_reduce_tasks"));
                job.setCreatedTime(rs.getTimestamp("created_time"));
                job.setStartTime(rs.getTimestamp("start_time"));
                job.setFinishTime(rs.getTimestamp("finish_time"));
                job.setInputBlobUrl(rs.getString("input_blob_url"));
                job.setOutputBlobUrl(rs.getString("output_blob_url"));
                
                // Handle potential null values for storage_type column
                String storageTypeStr = rs.getString("storage_type");
                if (storageTypeStr != null) {
                    job.setStorageType(Job.StorageType.valueOf(storageTypeStr));
                } else {
                    job.setStorageType(Job.StorageType.BLOB); // Default to BLOB
                }
                
                return job;
            }
            
            return null;
        } finally {
            DatabaseUtil.closeQuietly(rs);
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Get all jobs
     */
    public static List<Job> getAllJobs() throws SQLException {
        List<Job> jobs = new ArrayList<>();
        
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT * FROM jobs ORDER BY created_time DESC";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            
            while (rs.next()) {
                Job job = new Job();
                job.setJobId(rs.getString("job_id"));
                job.setStatus(Job.Status.valueOf(rs.getString("status")));
                job.setInputFile(rs.getString("input_file"));
                job.setNumMapTasks(rs.getInt("num_map_tasks"));
                job.setNumReduceTasks(rs.getInt("num_reduce_tasks"));
                job.setCreatedTime(rs.getTimestamp("created_time"));
                job.setStartTime(rs.getTimestamp("start_time"));
                job.setFinishTime(rs.getTimestamp("finish_time"));
                job.setInputBlobUrl(rs.getString("input_blob_url"));
                job.setOutputBlobUrl(rs.getString("output_blob_url"));
                
                // Handle potential null values for storage_type column
                String storageTypeStr = rs.getString("storage_type");
                if (storageTypeStr != null) {
                    job.setStorageType(Job.StorageType.valueOf(storageTypeStr));
                } else {
                    job.setStorageType(Job.StorageType.BLOB); // Default to BLOB
                }
                
                jobs.add(job);
            }
            
            return jobs;
        } finally {
            DatabaseUtil.closeQuietly(rs);
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Update job status
     */
    public static void updateJobStatus(String jobId, Job.Status status) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "UPDATE jobs SET status = ?";
            
            if (status == Job.Status.RUNNING) {
                sql += ", start_time = CURRENT_TIMESTAMP";
            } else if (status == Job.Status.COMPLETED || status == Job.Status.FAILED) {
                sql += ", finish_time = CURRENT_TIMESTAMP";
            }
            
            sql += " WHERE job_id = ?";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, status.name());
            stmt.setString(2, jobId);
            
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                logger.info("Updated job {} status to {}", jobId, status);
            } else {
                logger.warn("Job {} not found for status update", jobId);
            }
        } finally {
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Update job output blob URL
     */
    public static void updateJobOutputBlobUrl(String jobId, String outputBlobUrl) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "UPDATE jobs SET output_blob_url = ? WHERE job_id = ?";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, outputBlobUrl);
            stmt.setString(2, jobId);
            
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                logger.info("Updated job {} output blob URL: {}", jobId, outputBlobUrl);
            } else {
                logger.warn("Job {} not found for output blob URL update", jobId);
            }
        } finally {
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
}