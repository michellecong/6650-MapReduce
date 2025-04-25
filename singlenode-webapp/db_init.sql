-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS singlenode;

-- 使用数据库
USE singlenode;

-- 创建作业表
CREATE TABLE IF NOT EXISTS jobs (
                                    job_id VARCHAR(50) PRIMARY KEY,
                                    status VARCHAR(20) NOT NULL,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    started_at TIMESTAMP NULL,
                                    completed_at TIMESTAMP NULL,
                                    input_blob_url VARCHAR(255) NULL,
                                    output_blob_url VARCHAR(255) NULL
);

-- 创建词频统计表
CREATE TABLE IF NOT EXISTS word_counts (
                                           id INT AUTO_INCREMENT PRIMARY KEY,
                                           job_id VARCHAR(50) NOT NULL,
                                           word VARCHAR(255) NOT NULL,
                                           count INT NOT NULL,
                                           INDEX (job_id),
                                           UNIQUE KEY job_word_idx (job_id, word)
);