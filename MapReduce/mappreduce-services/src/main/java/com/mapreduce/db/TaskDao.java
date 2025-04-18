package com.mapreduce.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.common.TaskStatus;
import com.mapreduce.common.TaskType;

/**
 * 任务数据访问对象
 */
public class TaskDao {
    private static final Logger logger = LogManager.getLogger(TaskDao.class);
    
    /**
     * 创建任务
     */
    public static void createTask(Task task) throws SQLException {
        String sql = "INSERT INTO tasks (task_id, job_id, task_type, status, input_path, output_path, " +
                     "partition_id, input_blob_url, output_blob_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, task.getTaskId());
            stmt.setString(2, task.getJobId());
            stmt.setString(3, task.getTaskType().name());
            stmt.setString(4, task.getStatus().name());
            stmt.setString(5, task.getInputPath());
            stmt.setString(6, task.getOutputPath());
            
            if (task.getTaskType() == TaskType.REDUCE) {
                stmt.setInt(7, task.getPartitionId());
            } else {
                stmt.setNull(7, java.sql.Types.INTEGER);
            }
            
            // Blob相关字段
            stmt.setString(8, task.getInputBlobUrl());
            stmt.setString(9, task.getOutputBlobUrl());
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Created task: {}", task.getTaskId());
            } else {
                throw new SQLException("Failed to create task: " + task.getTaskId());
            }
        }
    }
    
    /**
     * 批量创建任务
     */
    public static void createTasks(List<Task> tasks) throws SQLException {
        String sql = "INSERT INTO tasks (task_id, job_id, task_type, status, input_path, output_path, " +
                     "partition_id, input_blob_url, output_blob_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            conn.setAutoCommit(false);
            
            for (Task task : tasks) {
                stmt.setString(1, task.getTaskId());
                stmt.setString(2, task.getJobId());
                stmt.setString(3, task.getTaskType().name());
                stmt.setString(4, task.getStatus().name());
                stmt.setString(5, task.getInputPath());
                stmt.setString(6, task.getOutputPath());
                
                if (task.getTaskType() == TaskType.REDUCE) {
                    stmt.setInt(7, task.getPartitionId());
                } else {
                    stmt.setNull(7, java.sql.Types.INTEGER);
                }
                
                // Blob相关字段
                stmt.setString(8, task.getInputBlobUrl());
                stmt.setString(9, task.getOutputBlobUrl());
                
                stmt.addBatch();
            }
            
            int[] results = stmt.executeBatch();
            conn.commit();
            
            logger.info("Created {} tasks", results.length);
        } catch (SQLException e) {
            logger.error("Error creating tasks", e);
            throw e;
        }
    }
    
    /**
     * 更新任务状态
     */
    public static void updateTaskStatus(String taskId, TaskStatus status, String workerId) throws SQLException {
        String sql = "UPDATE tasks SET status = ?";
        
        if (status == TaskStatus.RUNNING) {
            sql += ", start_time = CURRENT_TIMESTAMP, worker_id = ?";
        } else if (status == TaskStatus.COMPLETED || status == TaskStatus.FAILED) {
            sql += ", finish_time = CURRENT_TIMESTAMP";
        }
        
        sql += " WHERE task_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, status.name());
            
            if (status == TaskStatus.RUNNING) {
                stmt.setString(2, workerId);
                stmt.setString(3, taskId);
            } else {
                stmt.setString(2, taskId);
            }
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated task {} status to {}", taskId, status);
            } else {
                logger.warn("Task {} not found for status update", taskId);
            }
        }
    }
    
    /**
     * 更新任务的Blob输出URL
     */
    public static void updateTaskOutputBlobUrl(String taskId, String outputBlobUrl) throws SQLException {
        String sql = "UPDATE tasks SET output_blob_url = ? WHERE task_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, outputBlobUrl);
            stmt.setString(2, taskId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated task {} output blob URL: {}", taskId, outputBlobUrl);
            } else {
                logger.warn("Task {} not found for output blob URL update", taskId);
            }
        }
    }
    
    /**
     * 增加任务尝试次数
     */
    public static void incrementTaskAttempt(String taskId) throws SQLException {
        String sql = "UPDATE tasks SET attempt_count = attempt_count + 1 WHERE task_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, taskId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Incremented task {} attempt count", taskId);
            } else {
                logger.warn("Task {} not found for attempt count update", taskId);
            }
        }
    }
    
    /**
     * 获取任务
     */
    public static Task getTask(String taskId) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE task_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, taskId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToTask(rs);
                } else {
                    return null;
                }
            }
        }
    }
    
    /**
     * 获取作业的所有任务
     */
    public static List<Task> getTasksByJob(String jobId) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE job_id = ?";
        List<Task> tasks = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapResultSetToTask(rs));
                }
            }
        }
        
        return tasks;
    }
    
    /**
     * 获取指定 Worker 和状态的所有任务
     */
    public static List<Task> getTasksByWorkerAndStatus(String workerId, TaskStatus status) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE worker_id = ? AND status = ?";
        List<Task> tasks = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, workerId);
            stmt.setString(2, status.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapResultSetToTask(rs));
                }
            }
        }
        
        return tasks;
    }

    /**
     * 获取指定作业和类型的所有任务
     */
    public static List<Task> getTasksByJobAndType(String jobId, TaskType type) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE job_id = ? AND task_type = ?";
        List<Task> tasks = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setString(2, type.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapResultSetToTask(rs));
                }
            }
        }
        
        return tasks;
    }
    
    /**
     * 获取作业的所有特定类型和状态的任务
     */
    public static List<Task> getTasksByJobTypeAndStatus(String jobId, TaskType type, TaskStatus status) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE job_id = ? AND task_type = ? AND status = ?";
        List<Task> tasks = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setString(2, type.name());
            stmt.setString(3, status.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapResultSetToTask(rs));
                }
            }
        }
        
        return tasks;
    }
    
    /**
     * 检查作业的所有特定类型的任务是否都是特定状态
     */
    public static boolean areAllTasksInStatus(String jobId, TaskType type, TaskStatus status) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tasks WHERE job_id = ? AND task_type = ? AND status != ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setString(2, type.name());
            stmt.setString(3, status.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) == 0;
                }
                return false;
            }
        }
    }
    
    /**
     * 获取待处理的任务
     */
    public static List<Task> getPendingTasks(TaskType type, int limit) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE status = ? AND task_type = ? LIMIT ?";
        List<Task> tasks = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, TaskStatus.PENDING.name());
            stmt.setString(2, type.name());
            stmt.setInt(3, limit);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapResultSetToTask(rs));
                }
            }
        }
        
        return tasks;
    }
    
    /**
     * 获取长时间运行的任务
     */
    public static List<Task> getLongRunningTasks(int minutes) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE status = ? AND start_time < DATE_SUB(NOW(), INTERVAL ? MINUTE)";
        List<Task> tasks = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, TaskStatus.RUNNING.name());
            stmt.setInt(2, minutes);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapResultSetToTask(rs));
                }
            }
        }
        
        return tasks;
    }
    
/**
     * 获取特定类型任务的数量
     */
    public static int getTaskCountByType(TaskType type) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tasks WHERE task_type = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, type.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        }
    }
    
    /**
     * 获取特定状态任务的数量
     */
    public static int getTaskCountByStatus(TaskStatus status) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tasks WHERE status = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, status.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
        }
    }
    
    /**
     * 将 ResultSet 映射到 Task 对象
     */
    private static Task mapResultSetToTask(ResultSet rs) throws SQLException {
        Task task = new Task();
        task.setTaskId(rs.getString("task_id"));
        task.setJobId(rs.getString("job_id"));
        task.setTaskType(TaskType.valueOf(rs.getString("task_type")));
        task.setStatus(TaskStatus.valueOf(rs.getString("status")));
        task.setInputPath(rs.getString("input_path"));
        task.setOutputPath(rs.getString("output_path"));
        
        if (task.getTaskType() == TaskType.REDUCE) {
            task.setPartitionId(rs.getInt("partition_id"));
        }
        
        task.setAttemptCount(rs.getInt("attempt_count"));
        task.setWorkerId(rs.getString("worker_id"));
        
        // 读取Blob相关字段
        task.setInputBlobUrl(rs.getString("input_blob_url"));
        task.setOutputBlobUrl(rs.getString("output_blob_url"));
        
        // 根据是否有Blob URL确定存储类型
        String inputBlobUrl = task.getInputBlobUrl();
        String outputBlobUrl = task.getOutputBlobUrl();
        if ((inputBlobUrl != null && !inputBlobUrl.isEmpty()) || 
            (outputBlobUrl != null && !outputBlobUrl.isEmpty())) {
            task.setStorageType(StorageType.BLOB);
        } else {
            task.setStorageType(StorageType.LOCAL);
        }
        
        return task;
    }
}