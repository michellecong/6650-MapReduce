package com.mapreduce.master;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.common.TaskStatus;
import com.mapreduce.common.TaskType;
import com.mapreduce.db.TaskDao;
import com.mapreduce.messaging.MessageProducer;

/**
 * 任务调度器
 */
public class TaskScheduler {
    private static final Logger logger = LogManager.getLogger(TaskScheduler.class);
    
    private final MessageProducer messageProducer;
    
    /**
     * 创建任务调度器
     */
    public TaskScheduler(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }
    
    /**
     * 添加任务
     */
    public void addTask(Task task) throws IOException, SQLException {
        // 将任务保存到数据库
        TaskDao.createTask(task);
        
        // 发送任务到队列
        messageProducer.sendTask(task);
        
        logger.info("Task scheduled: {}", task.getTaskId());
    }
    
    /**
     * 批量添加任务
     */
    public void addTasks(List<Task> tasks) throws IOException, SQLException {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        
        // 将任务批量保存到数据库
        TaskDao.createTasks(tasks);
        
        // 发送任务到队列
        for (Task task : tasks) {
            messageProducer.sendTask(task);
        }
        
        logger.info("Batch scheduled {} tasks", tasks.size());
    }
    
    /**
     * 创建 Reduce 任务
     */
    public List<Task> createReduceTasks(String jobId, int numReduceTasks, String outputDir, StorageType storageType) throws IOException, SQLException {
        List<Task> reduceTasks = new ArrayList<>();
        
        // 强制使用BLOB存储类型
        storageType = StorageType.BLOB;
        
        for (int i = 0; i < numReduceTasks; i++) {
            String outputPath = outputDir + "/reduce_" + i + ".txt";
            
            // 创建BLOB存储任务
            Task reduceTask = Task.createBlobReduceTask(jobId, i, outputPath);
            
            reduceTasks.add(reduceTask);
        }
        
        // 添加所有 Reduce 任务
        addTasks(reduceTasks);
        
        logger.info("Created {} reduce tasks for job: {}, all using BLOB storage", reduceTasks.size(), jobId);
        return reduceTasks;
    }
    
    /**
     * 重新调度失败的任务
     */
    public void rescheduleFailedTask(String taskId) throws IOException, SQLException {
        Task task = TaskDao.getTask(taskId);
        if (task != null && task.getStatus() == TaskStatus.FAILED) {
            if (task.getAttemptCount() < ConfigManager.getMaxTaskRetries()) {
                // 增加尝试次数
                TaskDao.incrementTaskAttempt(taskId);
                task.incrementAttemptCount();
                
                // 重置任务状态为等待
                TaskDao.updateTaskStatus(taskId, TaskStatus.PENDING, null);
                task.setStatus(TaskStatus.PENDING);
                
                // 重新发送任务
                messageProducer.sendTask(task);
                
                logger.info("Task rescheduled: {}, attempt: {}", taskId, task.getAttemptCount());
            } else {
                logger.warn("Task {} failed after {} attempts, giving up", taskId, task.getAttemptCount());
            }
        }
    }
    
    /**
     * 处理长时间运行的任务
     */
    public void handleLongRunningTasks(int timeoutMinutes) throws IOException, SQLException {
        List<Task> longRunningTasks = TaskDao.getLongRunningTasks(timeoutMinutes);
        
        for (Task task : longRunningTasks) {
            logger.warn("Task {} is running too long, marking as failed", task.getTaskId());
            
            // 将任务标记为失败
            TaskDao.updateTaskStatus(task.getTaskId(), TaskStatus.FAILED, null);
            
            // 尝试重新调度
            rescheduleFailedTask(task.getTaskId());
        }
    }
    
    /**
     * 检查 Map 阶段是否完成
     */
    public boolean isMapPhaseCompleted(String jobId) throws SQLException {
        return TaskDao.areAllTasksInStatus(jobId, TaskType.MAP, TaskStatus.COMPLETED);
    }
    
    /**
     * 检查作业是否完成
     */
    public boolean isJobCompleted(String jobId) throws SQLException {
        boolean mapCompleted = TaskDao.areAllTasksInStatus(jobId, TaskType.MAP, TaskStatus.COMPLETED);
        boolean reduceCompleted = TaskDao.areAllTasksInStatus(jobId, TaskType.REDUCE, TaskStatus.COMPLETED);
        
        return mapCompleted && reduceCompleted;
    }
}