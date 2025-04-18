package com.mapreduce.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 词频统计结果数据访问对象
 */
public class WordCountDao {
    private static final Logger logger = LogManager.getLogger(WordCountDao.class);
    
    /**
     * 保存单个词频统计结果
     */
    public static void saveWordCount(String jobId, String word, int count) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "INSERT INTO word_counts (job_id, word, count) VALUES (?, ?, ?) " +
                         "ON DUPLICATE KEY UPDATE count = count + VALUES(count)";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            stmt.setString(2, word);
            stmt.setInt(3, count);
            
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                logger.debug("Saved word count: {} -> {} for job {}", word, count, jobId);
            } else {
                logger.warn("Failed to save word count for: {}", word);
            }
        } finally {
            DbUtils.closeQuietly(conn, stmt);
        }
    }
    
    /**
     * 批量保存词频统计结果
     */
    public static void saveWordCounts(String jobId, Map<String, Integer> wordCounts) throws SQLException {
        if (wordCounts == null || wordCounts.isEmpty()) {
            logger.warn("No word counts to save for job: {}", jobId);
            return;
        }
        
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "INSERT INTO word_counts (job_id, word, count) VALUES (?, ?, ?) " +
                         "ON DUPLICATE KEY UPDATE count = count + VALUES(count)";
            
            conn = DatabaseConfig.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            
            int batchSize = 0;
            int totalCount = 0;
            
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                
                // 跳过无效的词或计数
                if (word == null || word.isEmpty() || count <= 0) {
                    continue;
                }
                
                stmt.setString(1, jobId);
                stmt.setString(2, word);
                stmt.setInt(3, count);
                stmt.addBatch();
                
                batchSize++;
                totalCount++;
                
                // 每1000条提交一次
                if (batchSize >= 1000) {
                    stmt.executeBatch();
                    conn.commit();
                    batchSize = 0;
                    logger.debug("Committed batch of word counts for job {}, processed: {}/{}", 
                              jobId, totalCount, wordCounts.size());
                }
            }
            
            // 提交剩余的批次
            if (batchSize > 0) {
                stmt.executeBatch();
                conn.commit();
            }
            
            logger.info("Saved {} word counts for job {}", totalCount, jobId);
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    logger.error("Error rolling back transaction", ex);
                }
            }
            logger.error("Error saving word counts for job {}", jobId, e);
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
     * 获取作业的词频统计结果
     */
    public static Map<String, Integer> getWordCounts(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Map<String, Integer> wordCounts = new HashMap<>();
        
        try {
            String sql = "SELECT word, count FROM word_counts WHERE job_id = ? ORDER BY count DESC";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, jobId);
            stmt.setFetchSize(1000); // 设置较大的fetch size提高性能
            
            rs = stmt.executeQuery();
            int count = 0;
            
            while (rs.next()) {
                String word = rs.getString("word");
                int wordCount = rs.getInt("count");
                wordCounts.put(word, wordCount);
                count++;
                
                // 每10000条记录日志一次，避免过多日志
                if (count % 10000 == 0) {
                    logger.debug("Loaded {} word counts for job {}", count, jobId);
                }
            }
            
            logger.info("Retrieved {} word counts for job {}", wordCounts.size(), jobId);
            return wordCounts;
        } finally {
            DbUtils.closeQuietly(conn, stmt, rs);
        }
    }
    
    /**
     * 获取作业的前N个高频词
     */
    public static List<WordCount> getTopWords(String jobId, int limit) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<WordCount> topWords = new ArrayList<>();
        
        try {
            String sql = "SELECT word, count FROM word_counts WHERE job_id = ? ORDER BY count DESC LIMIT ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            stmt.setInt(2, limit);
            
            rs = stmt.executeQuery();
            while (rs.next()) {
                WordCount wordCount = new WordCount();
                wordCount.setWord(rs.getString("word"));
                wordCount.setCount(rs.getInt("count"));
                topWords.add(wordCount);
            }
            
            logger.info("Retrieved top {} words for job {}", topWords.size(), jobId);
            return topWords;
        } finally {
            DbUtils.closeQuietly(conn, stmt, rs);
        }
    }
    
    /**
     * 清除作业的词频统计结果
     */
    public static void clearWordCounts(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "DELETE FROM word_counts WHERE job_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("Cleared {} word counts for job {}", rowsAffected, jobId);
        } finally {
            DbUtils.closeQuietly(conn, stmt);
        }
    }
    
    /**
     * 获取作业的词频统计总数
     */
    public static int getWordCountTotal(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT COUNT(*) FROM word_counts WHERE job_id = ?";
            
            conn = DatabaseConfig.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, jobId);
            
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } finally {
            DbUtils.closeQuietly(conn, stmt, rs);
        }
    }
    
    /**
     * 词频统计类
     */
    public static class WordCount {
        private String word;
        private int count;
        
        public String getWord() {
            return word;
        }
        
        public void setWord(String word) {
            this.word = word;
        }
        
        public int getCount() {
            return count;
        }
        
        public void setCount(int count) {
            this.count = count;
        }
        
        @Override
        public String toString() {
            return word + "\t" + count;
        }
    }
}