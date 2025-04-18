-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS mapreduce;
USE mapreduce;

-- Jobs table - stores information about MapReduce jobs
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(50) PRIMARY KEY,
    status ENUM('PENDING', 'RUNNING', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING',
    input_file VARCHAR(255) NOT NULL,
    num_map_tasks INT NOT NULL DEFAULT 0,
    num_reduce_tasks INT NOT NULL DEFAULT 5,
    created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP NULL,
    finish_time TIMESTAMP NULL,
    input_blob_url VARCHAR(255) NULL COMMENT 'Input file Blob URL',
    output_blob_url VARCHAR(255) NULL COMMENT 'Final output Blob URL',
    storage_type ENUM('LOCAL', 'BLOB') NOT NULL DEFAULT 'LOCAL' COMMENT 'Storage type'
);

-- Tasks table - stores information about individual map and reduce tasks
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
    input_blob_url VARCHAR(255) NULL COMMENT 'Input data Blob URL',
    output_blob_url VARCHAR(255) NULL COMMENT 'Output data Blob URL',
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- Map output files table - stores intermediate results from Map tasks
CREATE TABLE IF NOT EXISTS map_outputs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(50) NOT NULL,
    job_id VARCHAR(50) NOT NULL,
    partition_id INT NOT NULL,
    file_path VARCHAR(255) NULL,
    blob_url VARCHAR(255) NULL COMMENT 'Intermediate result Blob URL',
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- Workers table - stores information about worker nodes
CREATE TABLE IF NOT EXISTS workers (
    worker_id VARCHAR(50) PRIMARY KEY,
    host VARCHAR(255) NOT NULL,
    status ENUM('ACTIVE', 'DEAD') NOT NULL DEFAULT 'ACTIVE',
    last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status)
);

-- Word count results table - stores word frequency results
CREATE TABLE IF NOT EXISTS word_counts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL,
    word VARCHAR(255) NOT NULL,
    count INT NOT NULL DEFAULT 0,
    UNIQUE KEY unique_word_job (job_id, word),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- Database update log table - tracks database schema changes
CREATE TABLE IF NOT EXISTS db_update_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_by VARCHAR(100) NOT NULL,
    script_name VARCHAR(255) NOT NULL
);

-- Create indexes for improved query performance
CREATE INDEX idx_task_status ON tasks(status);
CREATE INDEX idx_task_job_type ON tasks(job_id, task_type);
CREATE INDEX idx_worker_heartbeat ON workers(last_heartbeat);
CREATE INDEX idx_tasks_status_type ON tasks(status, task_type);
CREATE INDEX idx_word_counts_word ON word_counts(word);
CREATE INDEX idx_jobs_storage_type ON jobs(storage_type);
CREATE INDEX idx_map_outputs_blob_url ON map_outputs(blob_url);
CREATE INDEX idx_tasks_blob_urls ON tasks(input_blob_url, output_blob_url);

-- Stored procedure to kill sleep connections (for maintenance)
/*
DELIMITER $$
CREATE PROCEDURE KillSleepConnections(IN minutes INT)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE procId BIGINT;
    DECLARE procUser VARCHAR(64);
    DECLARE procHost VARCHAR(255);
    DECLARE cur CURSOR FOR
        SELECT ID, USER, HOST
        FROM information_schema.PROCESSLIST
        WHERE COMMAND = 'Sleep' AND TIME > minutes * 60 AND ID != CONNECTION_ID();

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO procId, procUser, procHost;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Log output (optional)
        SELECT CONCAT('Killing connection ID: ', procId, ', User: ', procUser, ', Host: ', procHost) AS message;

        -- Kill connection
        SET @kill_query = CONCAT('KILL ', procId);
        PREPARE stmt FROM @kill_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;
END$$
DELIMITER ;

-- Log initialization in update log
INSERT INTO db_update_log (version, description, applied_by, script_name)
VALUES ('v1.0.0', 'Initial schema creation with Blob storage support', CURRENT_USER(), 'init_schema.sql');

-- Display created tables
SHOW TABLES;

*/