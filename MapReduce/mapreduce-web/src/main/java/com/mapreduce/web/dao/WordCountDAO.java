package com.mapreduce.web.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.web.model.WordCount;
import com.mapreduce.web.util.DatabaseUtil;

/**
 * Data Access Object for word count data
 */
public class WordCountDAO {
    private static final Logger logger = LogManager.getLogger(WordCountDAO.class);
    
    /**
     * Get word count results for a job
     */
    public static List<WordCount> getWordCounts(String jobId, int limit) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<WordCount> wordCounts = new ArrayList<>();
        
        try {
            String sql = "SELECT word, count FROM word_counts WHERE job_id = ? ORDER BY count DESC";
            
            if (limit > 0) {
                sql += " LIMIT ?";
            }
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, jobId);
            
            if (limit > 0) {
                stmt.setInt(2, limit);
            }
            
            rs = stmt.executeQuery();
            
            while (rs.next()) {
                WordCount wordCount = new WordCount();
                wordCount.setWord(rs.getString("word"));
                wordCount.setCount(rs.getInt("count"));
                wordCounts.add(wordCount);
            }
            
            return wordCounts;
        } finally {
            DatabaseUtil.closeQuietly(rs);
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Get total number of unique words counted in a job
     */
    public static int getWordCountTotal(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT COUNT(*) FROM word_counts WHERE job_id = ?";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, jobId);
            
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                return rs.getInt(1);
            }
            
            return 0;
        } finally {
            DatabaseUtil.closeQuietly(rs);
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Get sum of all word counts in a job
     */
    public static int getTotalWordFrequency(String jobId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT SUM(count) FROM word_counts WHERE job_id = ?";
            
            conn = DatabaseUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, jobId);
            
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                return rs.getInt(1);
            }
            
            return 0;
        } finally {
            DatabaseUtil.closeQuietly(rs);
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
    
    /**
     * Save word count results for a job
     */
    public static void saveWordCounts(String jobId, List<WordCount> wordCounts) throws SQLException {
        if (wordCounts == null || wordCounts.isEmpty()) {
            logger.warn("No word counts to save for job: {}", jobId);
            return;
        }
        
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            String sql = "INSERT INTO word_counts (job_id, word, count) VALUES (?, ?, ?) " +
                         "ON DUPLICATE KEY UPDATE count = VALUES(count)";
            
            conn = DatabaseUtil.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            
            int batchCount = 0;
            for (WordCount wc : wordCounts) {
                stmt.setString(1, jobId);
                stmt.setString(2, wc.getWord());
                stmt.setInt(3, wc.getCount());
                stmt.addBatch();
                batchCount++;
                
                // Execute batch every 1000 records
                if (batchCount % 1000 == 0) {
                    stmt.executeBatch();
                    conn.commit();
                    batchCount = 0;
                }
            }
            
            // Execute remaining batch
            if (batchCount > 0) {
                stmt.executeBatch();
                conn.commit();
            }
            
            logger.info("Saved {} word counts for job: {}", wordCounts.size(), jobId);
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
                    logger.error("Error resetting auto-commit", e);
                }
            }
            DatabaseUtil.closeQuietly(stmt);
            DatabaseUtil.closeQuietly(conn);
        }
    }
}