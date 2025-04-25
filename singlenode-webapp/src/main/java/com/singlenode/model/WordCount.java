package com.singlenode.model;

/**
 * 词频统计实体类
 */
public class WordCount {
  private int id;
  private String jobId;
  private String word;
  private int count;

  /**
   * 默认构造函数
   */
  public WordCount() {
  }

  /**
   * 带参数的构造函数
   */
  public WordCount(String jobId, String word, int count) {
    this.jobId = jobId;
    this.word = word;
    this.count = count;
  }

  // Getters and Setters
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

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
    return "WordCount{" +
        "id=" + id +
        ", jobId='" + jobId + '\'' +
        ", word='" + word + '\'' +
        ", count=" + count +
        '}';
  }
}