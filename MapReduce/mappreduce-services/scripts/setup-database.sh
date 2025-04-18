#!/bin/bash

# 设置数据库脚本

# 默认数据库配置
DB_HOST="localhost"
DB_PORT="3306"
DB_NAME="mapreduce"
DB_USER="admin"
DB_PASSWORD="admin"

# 解析命令行参数
while getopts "h:p:n:u:w:" opt; do
  case $opt in
    h) DB_HOST="$OPTARG" ;;
    p) DB_PORT="$OPTARG" ;;
    n) DB_NAME="$OPTARG" ;;
    u) DB_USER="$OPTARG" ;;
    w) DB_PASSWORD="$OPTARG" ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
  esac
done

# 确保 MySQL 客户端已安装
if ! command -v mysql &> /dev/null; then
    echo "MySQL client is not installed. Please install it first."
    exit 1
fi

# 创建数据库
echo "Creating database: $DB_NAME"

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" <<EOF
CREATE DATABASE IF NOT EXISTS $DB_NAME;
USE $DB_NAME;

-- 作业表
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(50) PRIMARY KEY,
    status ENUM('PENDING', 'RUNNING', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING',
    input_file VARCHAR(255) NOT NULL,
    num_map_tasks INT NOT NULL DEFAULT 0,
    num_reduce_tasks INT NOT NULL DEFAULT 5,
    created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP NULL,
    finish_time TIMESTAMP NULL
);

-- 任务表
CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(50) PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL,
    task_type ENUM('MAP', 'REDUCE') NOT NULL,
    status ENUM('PENDING', 'RUNNING', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING',
    input_path VARCHAR(255),
    output_path VARCHAR(255),
    partition_id INT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    worker_id VARCHAR(50) NULL,
    created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP NULL,
    finish_time TIMESTAMP NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- Map 输出文件表
CREATE TABLE IF NOT EXISTS map_outputs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(50) NOT NULL,
    job_id VARCHAR(50) NOT NULL,
    partition_id INT NOT NULL,
    file_path VARCHAR(255) NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- Worker 节点表
CREATE TABLE IF NOT EXISTS workers (
    worker_id VARCHAR(50) PRIMARY KEY,
    host VARCHAR(255) NOT NULL,
    status ENUM('ACTIVE', 'DEAD') NOT NULL DEFAULT 'ACTIVE',
    last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status)
);

-- 词频结果表
CREATE TABLE IF NOT EXISTS word_counts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL,
    word VARCHAR(255) NOT NULL,
    count INT NOT NULL DEFAULT 0,
    UNIQUE KEY unique_word_job (job_id, word),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_task_job_type ON tasks(job_id, task_type);
CREATE INDEX IF NOT EXISTS idx_worker_heartbeat ON workers(last_heartbeat);
EOF

if [ $? -eq 0 ]; then
    echo "Database setup completed successfully!"
else
    echo "Failed to setup database. Please check your MySQL connection parameters."
    exit 1
fi
