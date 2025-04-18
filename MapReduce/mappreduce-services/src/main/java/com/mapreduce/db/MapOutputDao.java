package com.mapreduce.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Map 输出文件数据访问对象 - 适用于纯Blob存储
 */
public class MapOutputDao {
    private static final Logger logger = LogManager.getLogger(MapOutputDao.class);
    
    /**
     * 记录 Map 输出文件
     */
    public static void recordMapOutput(String taskId, String jobId, int partitionId, String filePath) throws SQLException {
        recordMapOutput(taskId, jobId, partitionId, filePath, null);
    }
    
    /**
     * 记录 Map 输出文件（含Blob URL）
     */
    public static void recordMapOutput(String taskId, String jobId, int partitionId, String filePath, String blobUrl) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        // 修复：如果使用纯Blob模式且filePath为null，则使用blobUrl作为filePath
        if (filePath == null && blobUrl != null) {
            // 使用blobUrl的一部分作为文件路径占位符
            filePath = "blob://" + partitionId + "/" + blobUrl.substring(blobUrl.lastIndexOf('/') + 1);
            logger.debug("Using blob URL as file path placeholder: {}", filePath);
        }
        
        try {
            String sql = "INSERT INTO map_outputs (task_id, job_id, partition_id, file_path, blob_url) VALUES (?, ?, ?, ?, ?)";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, taskId);
            stmt.setString(2, jobId);
            stmt.setInt(3, partitionId);
            stmt.setString(4, filePath);  // 现在不会是null
            stmt.setString(5, blobUrl);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.debug("Recorded map output for partition {}, blob URL: {}", partitionId, blobUrl);
            } else {
                throw new SQLException("Failed to record map output for partition: " + partitionId);
            }
        } finally {
            DbUtils.closeQuietly(conn, stmt);
        }
    }
    
    /**
     * 批量记录 Map 输出文件
     */
    public static void recordMapOutputs(String taskId, String jobId, List<MapOutput> outputs) throws SQLException {
        if (outputs == null || outputs.isEmpty()) {
            return;
        }
        
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "INSERT INTO map_outputs (task_id, job_id, partition_id, file_path, blob_url) VALUES (?, ?, ?, ?, ?)";
            
            conn = DatabaseConfig.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            
            for (MapOutput output : outputs) {
                String filePath = output.getFilePath();
                String blobUrl = output.getBlobUrl();
                
                // 修复：如果使用纯Blob模式且filePath为null，则使用blobUrl作为filePath
                if (filePath == null && blobUrl != null) {
                    // 使用blobUrl的一部分作为文件路径占位符
                    filePath = "blob://" + output.getPartitionId() + "/" + blobUrl.substring(blobUrl.lastIndexOf('/') + 1);
                }
                
                stmt.setString(1, taskId);
                stmt.setString(2, jobId);
                stmt.setInt(3, output.getPartitionId());
                stmt.setString(4, filePath);  // 现在不会是null
                stmt.setString(5, blobUrl);
                stmt.addBatch();
            }
            
            int[] results = stmt.executeBatch();
            conn.commit();
            
            logger.info("Recorded {} map outputs for task {}", results.length, taskId);
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    logger.error("Error rolling back transaction", ex);
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.warn("Error resetting auto-commit", e);
                }
            }
            DbUtils.closeQuietly(conn, stmt);
        }
    }
    
    /**
     * 获取作业特定分区的所有 Map 输出文件路径
     */
    public static List<String> getMapOutputFilePathsForPartition(String jobId, int partitionId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<String> filePaths = new ArrayList<>();
        
        try {
            String sql = "SELECT file_path FROM map_outputs WHERE job_id = ? AND partition_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            stmt.setInt(2, partitionId);
            
            rs = stmt.executeQuery();
            while (rs.next()) {
                String filePath = rs.getString("file_path");
                if (filePath != null && !filePath.isEmpty()) {
                    filePaths.add(filePath);
                }
            }
            
            return filePaths;
        } finally {
            DbUtils.closeQuietly(conn, stmt, rs);
        }
    }
    
    /**
     * 获取作业特定分区的所有 Map 输出
     */
    public static List<MapOutput> getMapOutputsForPartition(String jobId, int partitionId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<MapOutput> outputs = new ArrayList<>();
        
        try {
            String sql = "SELECT file_path, blob_url FROM map_outputs WHERE job_id = ? AND partition_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            stmt.setInt(2, partitionId);
            
            rs = stmt.executeQuery();
            while (rs.next()) {
                MapOutput output = new MapOutput();
                output.setPartitionId(partitionId);
                output.setFilePath(rs.getString("file_path"));
                output.setBlobUrl(rs.getString("blob_url"));
                outputs.add(output);
            }
            
            logger.info("Retrieved {} map outputs for job {}, partition {}", outputs.size(), jobId, partitionId);
            return outputs;
        } finally {
            DbUtils.closeQuietly(conn, stmt, rs);
        }
    }
    
    /**
     * 获取任务的所有 Map 输出文件
     */
    public static List<MapOutput> getMapOutputsForTask(String taskId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<MapOutput> outputs = new ArrayList<>();
        
        try {
            String sql = "SELECT partition_id, file_path, blob_url FROM map_outputs WHERE task_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, taskId);
            
            rs = stmt.executeQuery();
            while (rs.next()) {
                MapOutput output = new MapOutput();
                output.setPartitionId(rs.getInt("partition_id"));
                output.setFilePath(rs.getString("file_path"));
                output.setBlobUrl(rs.getString("blob_url"));
                outputs.add(output);
            }
            
            return outputs;
        } finally {
            DbUtils.closeQuietly(conn, stmt, rs);
        }
    }
    
    /**
     * 更新Map输出的Blob URL
     */
    public static void updateMapOutputBlobUrl(String taskId, int partitionId, String blobUrl) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "UPDATE map_outputs SET blob_url = ? WHERE task_id = ? AND partition_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, blobUrl);
            stmt.setString(2, taskId);
            stmt.setInt(3, partitionId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.debug("Updated map output blob URL for task {} partition {}: {}", taskId, partitionId, blobUrl);
            } else {
                logger.warn("No map output found for task {} partition {} to update blob URL", taskId, partitionId);
            }
        } finally {
            DbUtils.closeQuietly(conn, stmt);
        }
    }
    
    /**
     * 删除作业的所有Map输出记录
     */
    public static void deleteMapOutputsForJob(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "DELETE FROM map_outputs WHERE job_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("Deleted {} map outputs for job {}", rowsAffected, jobId);
        } finally {
            DbUtils.closeQuietly(conn, stmt);
        }
    }
    
    /**
     * Map 输出类
     */
    public static class MapOutput {
        private int partitionId;
        private String filePath;
        private String blobUrl;
        
        public MapOutput() {
        }
        
        public MapOutput(int partitionId, String filePath) {
            this.partitionId = partitionId;
            this.filePath = filePath;
        }
        
        public MapOutput(int partitionId, String filePath, String blobUrl) {
            this.partitionId = partitionId;
            this.filePath = filePath;
            this.blobUrl = blobUrl;
        }
        
        public int getPartitionId() {
            return partitionId;
        }
        
        public void setPartitionId(int partitionId) {
            this.partitionId = partitionId;
        }
        
        public String getFilePath() {
            return filePath;
        }
        
        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }
        
        public String getBlobUrl() {
            return blobUrl;
        }
        
        public void setBlobUrl(String blobUrl) {
            this.blobUrl = blobUrl;
        }
    }
}