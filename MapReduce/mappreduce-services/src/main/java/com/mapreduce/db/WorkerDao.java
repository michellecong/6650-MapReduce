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
 * Worker 节点数据访问对象
 */
public class WorkerDao {
    private static final Logger logger = LogManager.getLogger(WorkerDao.class);
    
    /**
     * 注册 Worker 节点
     */
    public static void registerWorker(String workerId, String host) throws SQLException {
        String sql = "INSERT INTO workers (worker_id, host, status) VALUES (?, ?, 'ACTIVE') " +
                     "ON DUPLICATE KEY UPDATE host = ?, last_heartbeat = CURRENT_TIMESTAMP, status = 'ACTIVE'";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, workerId);
            stmt.setString(2, host);
            stmt.setString(3, host);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Registered worker: {} at host: {}", workerId, host);
            } else {
                logger.warn("Failed to register worker: {}", workerId);
            }
        }
    }
    
    /**
     * 更新 Worker 心跳
     */
    public static void updateWorkerHeartbeat(String workerId) throws SQLException {
        String sql = "UPDATE workers SET last_heartbeat = CURRENT_TIMESTAMP WHERE worker_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, workerId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.debug("Updated heartbeat for worker: {}", workerId);
            } else {
                logger.warn("Worker not found for heartbeat update: {}", workerId);
            }
        }
    }
    
    /**
     * 标记 Worker 为失败状态
     */
    public static void markWorkerDead(String workerId) throws SQLException {
        String sql = "UPDATE workers SET status = 'DEAD' WHERE worker_id = ?";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, workerId);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Marked worker as dead: {}", workerId);
            } else {
                logger.warn("Worker not found for death marking: {}", workerId);
            }
        }
    }
    
    /**
     * 获取所有活跃的 Worker
     */
    public static List<WorkerInfo> getActiveWorkers() throws SQLException {
        String sql = "SELECT * FROM workers WHERE status = 'ACTIVE'";
        List<WorkerInfo> workers = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                WorkerInfo worker = new WorkerInfo();
                worker.setWorkerId(rs.getString("worker_id"));
                worker.setHost(rs.getString("host"));
                worker.setLastHeartbeat(rs.getTimestamp("last_heartbeat"));
                workers.add(worker);
            }
        }
        
        return workers;
    }
    
    /**
     * 获取心跳超时的 Worker
     */
    public static List<WorkerInfo> getTimeoutWorkers(int timeoutSeconds) throws SQLException {
        String sql = "SELECT * FROM workers WHERE status = 'ACTIVE' AND " +
                     "last_heartbeat < DATE_SUB(NOW(), INTERVAL ? SECOND)";
        List<WorkerInfo> workers = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, timeoutSeconds);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    WorkerInfo worker = new WorkerInfo();
                    worker.setWorkerId(rs.getString("worker_id"));
                    worker.setHost(rs.getString("host"));
                    worker.setLastHeartbeat(rs.getTimestamp("last_heartbeat"));
                    workers.add(worker);
                }
            }
        }
        
        return workers;
    }
    
    /**
     * 获取活跃的 Worker 数量
     */
    public static int getActiveWorkerCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM workers WHERE status = 'ACTIVE'";
        
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
     * 获取停用的 Worker 数量
     */
    public static int getDeadWorkerCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM workers WHERE status = 'DEAD'";
        
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
     * Worker 信息类
     */
    public static class WorkerInfo {
        private String workerId;
        private String host;
        private java.sql.Timestamp lastHeartbeat;
        
        public String getWorkerId() {
            return workerId;
        }
        
        public void setWorkerId(String workerId) {
            this.workerId = workerId;
        }
        
        public String getHost() {
            return host;
        }
        
        public void setHost(String host) {
            this.host = host;
        }
        
        public java.sql.Timestamp getLastHeartbeat() {
            return lastHeartbeat;
        }
        
        public void setLastHeartbeat(java.sql.Timestamp lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }
    }
}