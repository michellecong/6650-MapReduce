package com.singlenode.dao;

import com.singlenode.model.Job;
import com.singlenode.util.DatabaseConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 作业数据访问对象
 */
public class JobDao {
  private static final Logger logger = LogManager.getLogger(JobDao.class);

  /**
   * 创建作业表
   */
  public static void createJobTable() {
    String sql = "CREATE TABLE IF NOT EXISTS jobs (" +
        "job_id VARCHAR(50) PRIMARY KEY, " +
        "status VARCHAR(20) NOT NULL, " +
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
        "started_at TIMESTAMP NULL, " +
        "completed_at TIMESTAMP NULL, " +
        "input_blob_url VARCHAR(255) NULL, " +
        "output_blob_url VARCHAR(255) NULL" +
        ")";

    logger.info("Attempting to create jobs table with SQL: {}", sql);
    try (Connection conn = DatabaseConfig.getConnection();
         Statement stmt = conn.createStatement()) {
      boolean result = stmt.execute(sql);
      logger.info("Create table execution result: {}", result);
      logger.info("Created jobs table successfully");
    } catch (SQLException e) {
      logger.error("Error creating jobs table: {}", e.getMessage(), e);
      throw new RuntimeException("Error creating jobs table", e);
    }
  }

  /**
   * 创建新作业
   */
  public static String createJob(String inputBlobUrl) {
    String jobId = "job_" + System.currentTimeMillis();

    String sql = "INSERT INTO jobs (job_id, status, input_blob_url) VALUES (?, ?, ?)";

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, jobId);
      pstmt.setString(2, Job.Status.PENDING.name());

      // 处理可能为 null 的输入
      if (inputBlobUrl == null) {
        pstmt.setNull(3, java.sql.Types.VARCHAR);
      } else {
        pstmt.setString(3, inputBlobUrl);
      }

      pstmt.executeUpdate();

      logger.info("Created new job: {}", jobId);
      return jobId;
    } catch (SQLException e) {
      logger.error("Error creating job", e);
      throw new RuntimeException("Error creating job", e);
    }
  }

  /**
   * 更新作业状态
   */
  public static void updateJobStatus(String jobId, Job.Status status) {
    String sql = "UPDATE jobs SET status = ?";

    // 根据状态更新时间戳
    if (status == Job.Status.RUNNING) {
      sql += ", started_at = CURRENT_TIMESTAMP";
    } else if (status == Job.Status.COMPLETED || status == Job.Status.FAILED) {
      sql += ", completed_at = CURRENT_TIMESTAMP";
    }

    sql += " WHERE job_id = ?";

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, status.name());
      pstmt.setString(2, jobId);
      pstmt.executeUpdate();

      logger.info("Updated job status: {} -> {}", jobId, status);
    } catch (SQLException e) {
      logger.error("Error updating job status", e);
      throw new RuntimeException("Error updating job status", e);
    }
  }

  /**
   * 更新作业输出文件
   */
  public static void updateJobOutputBlobUrl(String jobId, String outputBlobUrl) {
    String sql = "UPDATE jobs SET output_blob_url = ? WHERE job_id = ?";

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {

      // 处理可能为 null 的输出
      if (outputBlobUrl == null) {
        pstmt.setNull(1, java.sql.Types.VARCHAR);
      } else {
        pstmt.setString(1, outputBlobUrl);
      }

      pstmt.setString(2, jobId);
      pstmt.executeUpdate();

      logger.info("Updated job output blob URL: {}", jobId);
    } catch (SQLException e) {
      logger.error("Error updating job output blob URL", e);
      throw new RuntimeException("Error updating job output blob URL", e);
    }
  }

  /**
   * 获取作业信息
   */
  public static Job getJob(String jobId) {
    String sql = "SELECT * FROM jobs WHERE job_id = ?";

    try (Connection conn = DatabaseConfig.getConnection();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, jobId);

      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          Job job = new Job();
          job.setJobId(rs.getString("job_id"));
          job.setStatus(Job.Status.valueOf(rs.getString("status")));
          job.setCreatedAt(getTimestampAsDate(rs, "created_at"));
          job.setStartedAt(getTimestampAsDate(rs, "started_at"));
          job.setCompletedAt(getTimestampAsDate(rs, "completed_at"));
          job.setInputBlobUrl(rs.getString("input_blob_url"));
          job.setOutputBlobUrl(rs.getString("output_blob_url"));
          return job;
        } else {
          logger.warn("Job not found: {}", jobId);
          return null;
        }
      }
    } catch (SQLException e) {
      logger.error("Error getting job", e);
      throw new RuntimeException("Error getting job", e);
    }
  }

  /**
   * 获取所有作业
   */
  public static List<Job> getAllJobs() {
    String sql = "SELECT * FROM jobs ORDER BY created_at DESC";
    List<Job> jobs = new ArrayList<>();

    try (Connection conn = DatabaseConfig.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      while (rs.next()) {
        Job job = new Job();
        job.setJobId(rs.getString("job_id"));
        job.setStatus(Job.Status.valueOf(rs.getString("status")));
        job.setCreatedAt(getTimestampAsDate(rs, "created_at"));
        job.setStartedAt(getTimestampAsDate(rs, "started_at"));
        job.setCompletedAt(getTimestampAsDate(rs, "completed_at"));
        job.setInputBlobUrl(rs.getString("input_blob_url"));
        job.setOutputBlobUrl(rs.getString("output_blob_url"));
        jobs.add(job);
      }

      return jobs;
    } catch (SQLException e) {
      logger.error("Error getting all jobs", e);
      throw new RuntimeException("Error getting all jobs", e);
    }
  }

  /**
   * 辅助方法：从ResultSet获取Timestamp并转换为Date
   */
  private static Date getTimestampAsDate(ResultSet rs, String columnName) throws SQLException {
    Timestamp timestamp = rs.getTimestamp(columnName);
    return (timestamp != null) ? new Date(timestamp.getTime()) : null;
  }
}