-- MapReduce 数据库表升级脚本
-- 用于添加 Blob 存储支持和优化数据库结构

-- 开始事务
START TRANSACTION;

-- 创建更新日志表（记录数据库修改历史）
CREATE TABLE IF NOT EXISTS db_update_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_by VARCHAR(100) NOT NULL,
    script_name VARCHAR(255) NOT NULL
);

-- 检查是否已经应用过此更新
SET @update_version = 'v1.1.0';
SET @update_description = 'Add Blob storage support';
SET @update_script = 'database_update.sql';
SET @update_applied_by = CURRENT_USER();

-- 如果日志表中没有此版本记录，则执行更新
IF NOT EXISTS (SELECT 1 FROM db_update_log WHERE version = @update_version) THEN

    -- 修改作业表，添加Blob存储相关字段
    ALTER TABLE jobs 
    ADD COLUMN input_blob_url VARCHAR(255) NULL COMMENT '输入文件的Blob URL',
    ADD COLUMN output_blob_url VARCHAR(255) NULL COMMENT '最终输出的Blob URL',
    ADD COLUMN storage_type ENUM('LOCAL', 'BLOB') NOT NULL DEFAULT 'LOCAL' COMMENT '存储类型';

    -- 修改任务表，添加Blob路径字段
    ALTER TABLE tasks 
    ADD COLUMN input_blob_url VARCHAR(255) NULL COMMENT '输入数据的Blob URL',
    ADD COLUMN output_blob_url VARCHAR(255) NULL COMMENT '输出数据的Blob URL';

    -- 修改Map输出表，添加Blob路径
    ALTER TABLE map_outputs 
    ADD COLUMN blob_url VARCHAR(255) NULL COMMENT '中间结果的Blob URL';

    -- 创建Blob配置表
    CREATE TABLE IF NOT EXISTS blob_config (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE,
        value VARCHAR(255) NOT NULL,
        description VARCHAR(255) NULL,
        updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );

    -- 插入默认Blob配置
    INSERT INTO blob_config (name, value, description) VALUES
    ('AZURE_STORAGE_CONNECTION_STRING', '', 'Azure Blob存储连接字符串'),
    ('AZURE_STORAGE_CONTAINER', 'mapreduce', 'Azure Blob存储容器名称'),
    ('BLOB_STORAGE_TYPE', 'AZURE', '使用的Blob存储类型(AZURE/AWS/GCP)'),
    ('BLOB_ENABLED', 'false', '是否启用Blob存储功能');

    -- 添加索引以提高查询性能
    CREATE INDEX idx_jobs_storage_type ON jobs(storage_type);
    CREATE INDEX idx_map_outputs_blob_url ON map_outputs(blob_url);
    CREATE INDEX idx_tasks_blob_urls ON tasks(input_blob_url, output_blob_url);
    
    -- 优化现有表结构
    -- 添加必要的索引
    CREATE INDEX IF NOT EXISTS idx_tasks_status_type ON tasks(status, task_type);
    CREATE INDEX IF NOT EXISTS idx_word_counts_word ON word_counts(word);
    
    -- 记录此次更新
    INSERT INTO db_update_log (version, description, applied_by, script_name)
    VALUES (@update_version, @update_description, @update_applied_by, @update_script);
    
    SELECT CONCAT('Database updated to version ', @update_version) AS 'Update Status';
ELSE
    SELECT CONCAT('Update to version ', @update_version, ' has already been applied') AS 'Update Status';
END IF;

-- 提交事务
COMMIT;

-- 显示更新后的表结构
SHOW TABLES;