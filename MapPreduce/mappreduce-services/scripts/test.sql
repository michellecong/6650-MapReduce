
select * from jobs ORDER BY created_time DESC;
select * from workers order by last_heartbeat DESC;
select * from tasks order by created_time DESC, input_path DESC;
select * from map_outputs ORDER BY job_id DESC, file_path DESC;
select * from word_counts ORDER BY job_id desc;
select * from db_update_log

/*
-- 清空所有表（按顺序删除，避免外键约束问题）
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE map_outputs;
TRUNCATE TABLE tasks;
TRUNCATE TABLE word_counts;
TRUNCATE TABLE jobs;
TRUNCATE TABLE workers;

SET FOREIGN_KEY_CHECKS = 1;

*/
SELECT COUNT(*) FROM information_schema.processlist;
SHOW STATUS LIKE 'Threads_connected';
SHOW VARIABLES LIKE 'max_connections';
SHOW PROCESSLIST;
SET GLOBAL max_connections = 1024;


CALL KillSleepConnections(0); -- 杀掉所有 SLEEP 超过 1 分钟的连接
SHOW PROCESSLIST;
--临时关闭 secure transport（即强制加密连接
SET GLOBAL require_secure_transport = OFF;
SET GLOBAL require_secure_transport = ON;


/*

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

        -- 输出日志（可选）
        SELECT CONCAT('Killing connection ID: ', procId, ', User: ', procUser, ', Host: ', procHost) AS message;

        -- Kill 连接
        SET @kill_query = CONCAT('KILL ', procId);
        PREPARE stmt FROM @kill_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;
END;

*/

