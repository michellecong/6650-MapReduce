package com.singlenode.dao;

import com.singlenode.util.DatabaseConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * 词频统计数据访问对象
 */
public class WordCountDao {
  private static final Logger logger = LogManager.getLogger(WordCountDao.class);

  /**
   * 创建词频统计表
   */
  public static void createWordCountTable() {
    String sql = "CREATE TABLE IF NOT EXISTS word_counts (" +
        "id INT AUTO_INCREMENT PRIMARY KEY, " +
        "job_id VARCHAR(50) NOT NULL, " +
        "word VARCHAR(255) NOT NULL, " +
        "count INT NOT NULL, " +
        "INDEX (job_id), " +
        "UNIQUE KEY job_word_idx (job_id, word)" +
        ")";

    try (Connection conn = DatabaseConfig.getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      logger.info("Created word_counts table");
    } catch (SQLException e) {
      logger.error("Error creating word_counts table", e);
      throw new RuntimeException("Error creating word_counts table", e);
    }
  }

  /**
   * 保存词频统计结果
   */
  public static void saveWordCounts(String jobId, Map<String, Integer> wordCounts) {
    String sql = "INSERT INTO word_counts (job_id, word, count) VALUES (?, ?, ?) " +
        "ON DUPLICATE KEY UPDATE count = VALUES(count)";

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {

      // 启用批处理
      conn.setAutoCommit(false);

      int count = 0;
      for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
        pstmt.setString(1, jobId);
        pstmt.setString(2, entry.getKey());
        pstmt.setInt(3, entry.getValue());
        pstmt.addBatch();

        count++;

        // 每1000条记录执行一次批处理
        if (count % 1000 == 0) {
          pstmt.executeBatch();
          logger.info("Saved {} word counts in batch", count);
        }
      }

      // 执行剩余的批处理
      pstmt.executeBatch();
      conn.commit();

      logger.info("Saved total {} word counts for job {}", wordCounts.size(), jobId);
    } catch (SQLException e) {
      logger.error("Error saving word counts", e);
      throw new RuntimeException("Error saving word counts", e);
    }
  }

  /**
   * 获取作业的词频统计结果
   */
  public static Map<String, Integer> getWordCounts(String jobId) {
    String sql = "SELECT word, count FROM word_counts WHERE job_id = ?";
    Map<String, Integer> wordCounts = new HashMap<>();

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, jobId);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String word = rs.getString("word");
          int count = rs.getInt("count");
          wordCounts.put(word, count);
        }
      }

      logger.info("Retrieved {} word counts for job {}", wordCounts.size(), jobId);
      return wordCounts;
    } catch (SQLException e) {
      logger.error("Error getting word counts", e);
      throw new RuntimeException("Error getting word counts", e);
    }
  }

  /**
   * 获取作业的前N个高频词
   */
  public static Map<String, Integer> getTopWordCounts(String jobId, int limit) {
    String sql = "SELECT word, count FROM word_counts WHERE job_id = ? " +
        "ORDER BY count DESC LIMIT ?";
    Map<String, Integer> wordCounts = new HashMap<>();

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, jobId);
      pstmt.setInt(2, limit);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String word = rs.getString("word");
          int count = rs.getInt("count");
          wordCounts.put(word, count);
        }
      }

      logger.info("Retrieved top {} word counts for job {}", wordCounts.size(), jobId);
      return wordCounts;
    } catch (SQLException e) {
      logger.error("Error getting top word counts", e);
      throw new RuntimeException("Error getting top word counts", e);
    }
  }
}