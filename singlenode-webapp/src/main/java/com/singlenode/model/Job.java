package com.singlenode.model;

import java.util.Date;

/**
 * 任务实体类
 */
public class Job {
  private String jobId;
  private Status status;
  private Date createdAt;
  private Date startedAt;
  private Date completedAt;
  private String inputBlobUrl;
  private String outputBlobUrl;

  /**
   * 任务状态枚举
   */
  public enum Status {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED
  }

  // Getters and Setters
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  public Date getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(Date startedAt) {
    this.startedAt = startedAt;
  }

  public Date getCompletedAt() {
    return completedAt;
  }

  public void setCompletedAt(Date completedAt) {
    this.completedAt = completedAt;
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