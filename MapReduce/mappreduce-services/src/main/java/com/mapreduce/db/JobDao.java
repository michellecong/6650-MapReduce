package com.mapreduce.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.JobStatus;
import com.mapreduce.common.StorageType;

/**
 * 作业数据访问对象
 */
public class JobDao {
    private static final Logger logger = LogManager.getLogger(JobDao.class);
    
    /**
     * 创建新作业
     */
    public static String createJob(String inputFile, int numReduceTasks) throws SQLException {
        return createJob(inputFile, numReduceTasks, StorageType.LOCAL, null);
    }
    
    /**
     * 创建新作业，支持Blob存储
     */
    public static String createJob(String inputFile, int numReduceTasks, StorageType storageType, String inputBlobUrl) throws SQLException {
        String jobId = "job_" + System.currentTimeMillis();
        String sql = "INSERT INTO jobs (job_id, input_file, num_reduce_tasks, status, storage_type, input_blob_url) VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setString(2, inputFile);
            stmt.setInt(3, numReduceTasks);
            stmt.setString(4, JobStatus.PENDING.name());
            stmt.setString(5, storageType.name());
            stmt.setString(6, inputBlobUrl);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Created job: {} with storage type: {}", jobId, storageType);
                return jobId;
            } else {
                throw new SQLException("Failed to create job");
            }
        }
    }
    
    /**
     * 更新作业状态
     */
    public static void updateJobStatus(String jobId, JobStatus status) throws SQLException {
        String sql = "UPDATE jobs SET status = ?";
        
        if (status == JobStatus.RUNNING) {
            sql += ", start_time = CURRENT_TIMESTAMP";
        } else if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
            sql += ", finish_time = CURRENT_TIMESTAMP";
        }
        
        sql += " WHERE job_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, status.name());
            stmt.setString(2, jobId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated job {} status to {}", jobId, status);
            } else {
                logger.warn("Job {} not found for status update", jobId);
            }
        }
    }
    
    /**
     * 更新作业的输出Blob URL
     */
    public static void updateJobOutputBlobUrl(String jobId, String outputBlobUrl) throws SQLException {
        String sql = "UPDATE jobs SET output_blob_url = ? WHERE job_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, outputBlobUrl);
            stmt.setString(2, jobId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated job {} output blob URL: {}", jobId, outputBlobUrl);
            } else {
                logger.warn("Job {} not found for output blob URL update", jobId);
            }
        }
    }
    
    /**
     * 更新作业的 Map 任务数量
     */
    public static void updateMapTaskCount(String jobId, int numMapTasks) throws SQLException {
        String sql = "UPDATE jobs SET num_map_tasks = ? WHERE job_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, numMapTasks);
            stmt.setString(2, jobId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated job {} map task count to {}", jobId, numMapTasks);
            } else {
                logger.warn("Job {} not found for map task count update", jobId);
            }
        }
    }
    
    /**
     * 获取作业状态
     */
    public static JobStatus getJobStatus(String jobId) throws SQLException {
        String sql = "SELECT status FROM jobs WHERE job_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return JobStatus.valueOf(rs.getString("status"));
                } else {
                    throw new SQLException("Job not found: " + jobId);
                }
            }
        }
    }
    
    /**
     * 获取作业信息
     */
    public static JobInfo getJobInfo(String jobId) throws SQLException {
        String sql = "SELECT * FROM jobs WHERE job_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    JobInfo jobInfo = new JobInfo();
                    jobInfo.setJobId(rs.getString("job_id"));
                    jobInfo.setStatus(JobStatus.valueOf(rs.getString("status")));
                    jobInfo.setInputFile(rs.getString("input_file"));
                    jobInfo.setNumMapTasks(rs.getInt("num_map_tasks"));
                    jobInfo.setNumReduceTasks(rs.getInt("num_reduce_tasks"));
                    jobInfo.setCreatedTime(rs.getTimestamp("created_time"));
                    jobInfo.setStartTime(rs.getTimestamp("start_time"));
                    jobInfo.setFinishTime(rs.getTimestamp("finish_time"));
                    
                    // 读取Blob相关字段
                    jobInfo.setStorageType(StorageType.valueOf(rs.getString("storage_type")));
                    jobInfo.setInputBlobUrl(rs.getString("input_blob_url"));
                    jobInfo.setOutputBlobUrl(rs.getString("output_blob_url"));
                    
                    return jobInfo;
                } else {
                    throw new SQLException("Job not found: " + jobId);
                }
            }
        }
    }
    
    /**
     * 获取所有作业
     */
    public static List<JobInfo> getAllJobs() throws SQLException {
        String sql = "SELECT * FROM jobs ORDER BY created_time DESC";
        List<JobInfo> jobs = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                JobInfo jobInfo = new JobInfo();
                jobInfo.setJobId(rs.getString("job_id"));
                jobInfo.setStatus(JobStatus.valueOf(rs.getString("status")));
                jobInfo.setInputFile(rs.getString("input_file"));
                jobInfo.setNumMapTasks(rs.getInt("num_map_tasks"));
                jobInfo.setNumReduceTasks(rs.getInt("num_reduce_tasks"));
                jobInfo.setCreatedTime(rs.getTimestamp("created_time"));
                jobInfo.setStartTime(rs.getTimestamp("start_time"));
                jobInfo.setFinishTime(rs.getTimestamp("finish_time"));
                
                // 读取Blob相关字段
                jobInfo.setStorageType(StorageType.valueOf(rs.getString("storage_type")));
                jobInfo.setInputBlobUrl(rs.getString("input_blob_url"));
                jobInfo.setOutputBlobUrl(rs.getString("output_blob_url"));
                
                jobs.add(jobInfo);
            }
        }
        
        return jobs;
    }
    
    /**
     * 获取所有运行中的作业
     */
    public static List<JobInfo> getRunningJobs() throws SQLException {
        String sql = "SELECT * FROM jobs WHERE status = ?";
        List<JobInfo> jobs = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.RUNNING.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    JobInfo jobInfo = new JobInfo();
                    jobInfo.setJobId(rs.getString("job_id"));
                    jobInfo.setStatus(JobStatus.valueOf(rs.getString("status")));
                    jobInfo.setInputFile(rs.getString("input_file"));
                    jobInfo.setNumMapTasks(rs.getInt("num_map_tasks"));
                    jobInfo.setNumReduceTasks(rs.getInt("num_reduce_tasks"));
                    jobInfo.setCreatedTime(rs.getTimestamp("created_time"));
                    jobInfo.setStartTime(rs.getTimestamp("start_time"));
                    jobInfo.setFinishTime(rs.getTimestamp("finish_time"));
                    
                    // 读取Blob相关字段
                    jobInfo.setStorageType(StorageType.valueOf(rs.getString("storage_type")));
                    jobInfo.setInputBlobUrl(rs.getString("input_blob_url"));
                    jobInfo.setOutputBlobUrl(rs.getString("output_blob_url"));
                    
                    jobs.add(jobInfo);
                }
            }
        }
        
        return jobs;
    }
    
    /**
     * 获取作业总数
     */
    public static int getTotalJobCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM jobs";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
    
    /**
     * 获取运行中的作业数量
     */
    public static int getRunningJobCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM jobs WHERE status = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.RUNNING.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        }
    }
    
    /**
     * 获取已完成的作业数量
     */
    public static int getCompletedJobCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM jobs WHERE status = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.COMPLETED.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        }
    }
    
    /**
     * 获取失败的作业数量
     */
    public static int getFailedJobCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM jobs WHERE status = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.FAILED.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        }
    }
    
    /**
     * 作业信息类
     */
    public static class JobInfo {
        private String jobId;
        private JobStatus status;
        private String inputFile;
        private int numMapTasks;
        private int numReduceTasks;
        private Timestamp createdTime;
        private Timestamp startTime;
        private Timestamp finishTime;
        private StorageType storageType;
        private String inputBlobUrl;
        private String outputBlobUrl;
        
        // Getters and setters
        public String getJobId() {
            return jobId;
        }
        
        public void setJobId(String jobId) {
            this.jobId = jobId;
        }
        
        public JobStatus getStatus() {
            return status;
        }
        
        public void setStatus(JobStatus status) {
            this.status = status;
        }
        
        public String getInputFile() {
            return inputFile;
        }
        
        public void setInputFile(String inputFile) {
            this.inputFile = inputFile;
        }
        
        public int getNumMapTasks() {
            return numMapTasks;
        }
        
        public void setNumMapTasks(int numMapTasks) {
            this.numMapTasks = numMapTasks;
        }
        
        public int getNumReduceTasks() {
            return numReduceTasks;
        }
        
        public void setNumReduceTasks(int numReduceTasks) {
            this.numReduceTasks = numReduceTasks;
        }
        
        public Timestamp getCreatedTime() {
            return createdTime;
        }
        
        public void setCreatedTime(Timestamp createdTime) {
            this.createdTime = createdTime;
        }
        
        public Timestamp getStartTime() {
            return startTime;
        }
        
        public void setStartTime(Timestamp startTime) {
            this.startTime = startTime;
        }
        
        public Timestamp getFinishTime() {
            return finishTime;
        }
        
        public void setFinishTime(Timestamp finishTime) {
            this.finishTime = finishTime;
        }
        
        public StorageType getStorageType() {
            return storageType;
        }
        
        public void setStorageType(StorageType storageType) {
            this.storageType = storageType;
        }
        
        public String getInputBlobUrl() {
            return inputBlobUrl;
        }
        
        public void setInputBlobUrl(String inputBlobUrl) {
            this.inputBlobUrl = inputBlobUrl;
        }
        
        public String getOutputBlobUrl() {
            return outputBlobUrl;
        }
        
        public void setOutputBlobUrl(String outputBlobUrl) {
            this.outputBlobUrl = outputBlobUrl;
        }
    }
}