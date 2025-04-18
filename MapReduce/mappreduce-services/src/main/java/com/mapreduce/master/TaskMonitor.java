package com.mapreduce.master;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapreduce.common.ConfigManager;
import com.mapreduce.common.JobStatus;
import com.mapreduce.common.StorageType;
import com.mapreduce.common.Task;
import com.mapreduce.common.TaskStatus;
import com.mapreduce.common.TaskType;
import com.mapreduce.db.JobDao;
import com.mapreduce.db.MapOutputDao;
import com.mapreduce.db.TaskDao;
import com.mapreduce.db.WorkerDao;
import com.mapreduce.messaging.MessageConsumer;
import com.mapreduce.messaging.MessageProducer;
import com.mapreduce.storage.StorageManager;

/**
 * 任务监控器
 */
public class TaskMonitor {
    private static final Logger logger = LogManager.getLogger(TaskMonitor.class);
    
    private final TaskScheduler taskScheduler;
    private final MessageConsumer messageConsumer;
    private final StorageManager storageManager;
    private final ScheduledExecutorService scheduler;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    /**
     * 创建任务监控器
     */
    public TaskMonitor(TaskScheduler taskScheduler, MessageConsumer messageConsumer, StorageManager storageManager) {
        this.taskScheduler = taskScheduler;
        this.messageConsumer = messageConsumer;
        this.storageManager = storageManager;
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "task-monitor-thread");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * 启动监控
     */
    public void start() throws IOException {
        if (running.getAndSet(true)) {
            logger.warn("Task monitor already started");
            return;
        }
        
        // 消费 Map 结果
        messageConsumer.consumeMapResults(mapResult -> {
            handleMapResult(mapResult);
        });
        
        // 消费 Reduce 结果
        messageConsumer.consumeReduceResults(reduceResult -> {
            handleReduceResult(reduceResult);
        });
        
        // 消费心跳消息
        messageConsumer.consumeStatusMessages(heartbeat -> {
            handleHeartbeat(heartbeat);
        });
        
        // 启动定时任务监控
        scheduler.scheduleAtFixedRate(this::monitorTasks, 
                ConfigManager.getTaskMonitorIntervalSeconds(), 
                ConfigManager.getTaskMonitorIntervalSeconds(), 
                TimeUnit.SECONDS);
        
        logger.info("Task monitor started");
    }
        
    /**
     * 处理 Map 任务结果 - 修改为正确提取分区ID
     */
    private void handleMapResult(MessageProducer.MapResultMessage mapResult) throws SQLException {
        String taskId = mapResult.getTaskId();
        String jobId = mapResult.getJobId();
        List<String> blobUrls = mapResult.getBlobUrls();
        
        // 安全检查
        if (taskId == null || jobId == null) {
            logger.error("Received invalid map result message: missing taskId or jobId");
            return;
        }
        
        logger.info("Handling map result for task: {}, with {} blob URLs", taskId, 
                blobUrls != null ? blobUrls.size() : 0);
        
        // 更新任务状态
        TaskDao.updateTaskStatus(taskId, TaskStatus.COMPLETED, null);
        
        // 获取任务，检查存储类型
        Task task = TaskDao.getTask(taskId);
        if (task == null) {
            logger.error("Task not found: {}", taskId);
            return;
        }
        
        // 确保使用Blob存储
        if (task.getStorageType() != StorageType.BLOB) {
            logger.warn("Task {} is not using Blob storage, setting to BLOB", taskId);
            task.setStorageType(StorageType.BLOB);
        }
        
        // 记录Map输出的Blob URLs到数据库
        if (blobUrls != null && !blobUrls.isEmpty()) {
            for (String blobUrl : blobUrls) {
                if (blobUrl != null && !blobUrl.isEmpty()) {
                    // 从blob URL提取分区ID
                    int partitionId = extractPartitionIdFromBlobUrl(blobUrl);
                    logger.info("Recording map output for partition {}: {}", partitionId, blobUrl);
                    
                    // 记录到数据库（分区ID来自URL）
                    MapOutputDao.recordMapOutput(taskId, jobId, partitionId, null, blobUrl);
                }
            }
        } else {
            logger.warn("No blob URLs found in map result for task: {}", taskId);
        }
        
        // 检查 Map 阶段是否完成
        checkMapPhaseCompletion(jobId);
    }

    /**
     * 从Blob URL中提取分区ID
     */
    private int extractPartitionIdFromBlobUrl(String blobUrl) {
        try {
            // 查找URL中的part_X.txt模式
            int partIndex = blobUrl.lastIndexOf("/part_");
            if (partIndex != -1) {
                // 从URL中提取分区号（part_后面，.txt前面的数字）
                int startIndex = partIndex + 6; // "/part_"的长度是6
                int endIndex = blobUrl.indexOf(".txt", startIndex);
                if (endIndex != -1) {
                    String partIdStr = blobUrl.substring(startIndex, endIndex);
                    int partitionId = Integer.parseInt(partIdStr);
                    logger.info("Successfully extracted partition ID {} from URL: {}", partitionId, blobUrl);
                    return partitionId;
                }
            }
        } catch (Exception e) {
            logger.error("Error extracting partition ID from URL: " + blobUrl, e);
        }
        
        logger.warn("Could not extract partition ID from URL, defaulting to 0: {}", blobUrl);
        return 0;
    }
    /**
     * 处理 Reduce 任务结果
     */
    private void handleReduceResult(MessageProducer.ReduceResultMessage reduceResult) throws SQLException, IOException {
        String taskId = reduceResult.getTaskId();
        String jobId = reduceResult.getJobId();
        String blobUrl = reduceResult.getBlobUrl();
        
        logger.info("Handling reduce result for task: {}, blob URL: {}", taskId, blobUrl);
        
        // 更新任务状态
        TaskDao.updateTaskStatus(taskId, TaskStatus.COMPLETED, null);
        
        // 获取任务，检查存储类型
        Task task = TaskDao.getTask(taskId);
        if (task == null) {
            logger.error("Task not found: {}", taskId);
            return;
        }
        
        // 更新任务的Blob URL
        if (blobUrl != null && !blobUrl.isEmpty()) {
            TaskDao.updateTaskOutputBlobUrl(taskId, blobUrl);
        } else {
            logger.warn("No blob URL provided for task: {}", taskId);
            return; // 没有Blob URL，无法继续处理
        }
        
        // 直接从Blob加载数据到数据库
        try {
            storageManager.loadReduceOutputToDatabase(jobId, null, StorageType.BLOB, blobUrl);
        } catch (Exception e) {
            logger.error("Failed to load reduce output to database from Blob: {}", e.getMessage());
        }
        
        // 检查作业是否完成
        checkJobCompletion(jobId);
    }
    
    /**
     * 处理 Worker 心跳
     */
    private void handleHeartbeat(MessageProducer.HeartbeatMessage heartbeat) throws SQLException {
        String workerId = heartbeat.getWorkerId();
        String host = heartbeat.getHost();
        
        // 更新 Worker 心跳
        WorkerDao.registerWorker(workerId, host);
    }
    
    /**
     * 监控任务状态
     */
    private void monitorTasks() {
        try {
            // 检查超时的 Worker
            List<WorkerDao.WorkerInfo> timeoutWorkers = WorkerDao.getTimeoutWorkers(ConfigManager.getWorkerTimeoutSeconds());
            for (WorkerDao.WorkerInfo worker : timeoutWorkers) {
                handleWorkerTimeout(worker.getWorkerId());
            }
            
            // 检查长时间运行的任务
            taskScheduler.handleLongRunningTasks(5); // 5分钟
            
            // 检查运行中的作业
            List<JobDao.JobInfo> runningJobs = JobDao.getRunningJobs();
            for (JobDao.JobInfo job : runningJobs) {
                checkMapPhaseCompletion(job.getJobId());
                checkJobCompletion(job.getJobId());
            }
            
            // 记录系统状态日志
            logSystemStatus();
        } catch (Exception e) {
            logger.error("Error in task monitor", e);
        }
    }
    
    /**
     * 记录系统状态
     */
    private void logSystemStatus() {
        try {
            int activeWorkers = WorkerDao.getActiveWorkerCount();
            int deadWorkers = WorkerDao.getDeadWorkerCount();
            int pendingTasks = TaskDao.getTaskCountByStatus(TaskStatus.PENDING);
            int runningTasks = TaskDao.getTaskCountByStatus(TaskStatus.RUNNING);
            int completedTasks = TaskDao.getTaskCountByStatus(TaskStatus.COMPLETED);
            int failedTasks = TaskDao.getTaskCountByStatus(TaskStatus.FAILED);
            int runningJobs = JobDao.getRunningJobCount();
            
            logger.info("System status: Workers [active={}, dead={}], " +
                      "Tasks [pending={}, running={}, completed={}, failed={}], " +
                      "Jobs [running={}]",
                      activeWorkers, deadWorkers,
                      pendingTasks, runningTasks, completedTasks, failedTasks,
                      runningJobs);
        } catch (Exception e) {
            logger.warn("Error logging system status", e);
        }
    }
    
    /**
     * 处理 Worker 超时
     */
    private void handleWorkerTimeout(String workerId) throws SQLException {
        logger.warn("Worker {} timed out", workerId);
        
        // 将 Worker 标记为失败
        WorkerDao.markWorkerDead(workerId);
        
        // 查找 Worker 正在执行的任务
        List<Task> tasks = TaskDao.getTasksByWorkerAndStatus(workerId, TaskStatus.RUNNING);
        
        for (Task task : tasks) {
            logger.warn("Task {} was running on failed worker {}, marking as failed", task.getTaskId(), workerId);
            
            // 将任务标记为失败
            TaskDao.updateTaskStatus(task.getTaskId(), TaskStatus.FAILED, null);
            
            // 尝试重新调度任务
            try {
                taskScheduler.rescheduleFailedTask(task.getTaskId());
            } catch (Exception e) {
                logger.error("Error rescheduling task: {}", task.getTaskId(), e);
            }
        }
    }
    
    /**
     * 检查 Map 阶段是否完成
     */
    private void checkMapPhaseCompletion(String jobId) throws SQLException {
        // 检查所有 Map 任务是否完成
        if (taskScheduler.isMapPhaseCompleted(jobId)) {
            logger.info("Map phase completed for job: {}", jobId);
            
            // 检查 Reduce 任务是否已创建
            List<Task> reduceTasks = TaskDao.getTasksByJobAndType(jobId, TaskType.REDUCE);
            
            if (reduceTasks.isEmpty()) {
                logger.info("Creating reduce tasks for job: {}", jobId);
                
                try {
                    // 获取作业信息
                    JobDao.JobInfo jobInfo = JobDao.getJobInfo(jobId);
                    
                    // 创建 Reduce 任务
                    String outputDir = storageManager.getOutputDirectory(jobId);
                    taskScheduler.createReduceTasks(jobId, jobInfo.getNumReduceTasks(), outputDir, StorageType.BLOB);
                    
                    // 等待一小段时间确保 Map 输出在数据库中已经可用
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } catch (Exception e) {
                    logger.error("Error creating reduce tasks for job: {}", jobId, e);
                }
            }
        }
    }
    
    /**
     * 检查作业是否完成
     */
    private void checkJobCompletion(String jobId) throws SQLException {
        // 检查所有任务是否完成
        if (taskScheduler.isJobCompleted(jobId)) {
            logger.info("All tasks completed for job: {}", jobId);
            
            try {
                // 获取作业信息
                JobDao.JobInfo jobInfo = JobDao.getJobInfo(jobId);
                
                // 如果作业已经标记为完成，不需要再处理
                if (jobInfo.getStatus() == JobStatus.COMPLETED) {
                    logger.info("Job {} already marked as completed", jobId);
                    return;
                }
                
                // 更新作业状态
                JobDao.updateJobStatus(jobId, JobStatus.COMPLETED);
                
                // 合并结果 - 只从数据库读取，直接上传至Blob
                String outputFile = "wordcount_" + jobId + ".txt";
                String blobUrl = storageManager.mergeOutputFiles(jobId, outputFile, StorageType.BLOB);
                
                // 更新作业的输出Blob URL
                if (blobUrl != null && blobUrl.startsWith("http")) {
                    JobDao.updateJobOutputBlobUrl(jobId, blobUrl);
                    logger.info("Updated job output Blob URL: {}", blobUrl);
                }
                
                // 清理临时文件和不需要的Blob
                storageManager.cleanup(jobId);
                
                logger.info("Job {} completed successfully", jobId);
            } catch (Exception e) {
                logger.error("Error finalizing job: {}", jobId, e);
                try {
                    JobDao.updateJobStatus(jobId, JobStatus.FAILED);
                } catch (SQLException ex) {
                    logger.error("Error updating job status to FAILED", ex);
                }
            }
        }
    }
    
    /**
     * 从文件路径中提取分区 ID
     * (保留此方法以兼容现有代码，但实际上不再使用)
     */
    private int extractPartitionId(String filePath) {
        try {
            String fileName = new File(filePath).getName();
            int startIndex = fileName.lastIndexOf('_');
            int endIndex = fileName.lastIndexOf('.');
            if (startIndex >= 0 && endIndex > startIndex) {
                return Integer.parseInt(fileName.substring(startIndex + 1, endIndex));
            }
        } catch (Exception e) {
            logger.error("Error extracting partition ID from file path: {}", filePath, e);
        }
        return 0;
    }
    
    /**
     * 停止监控
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            logger.warn("Task monitor already stopped");
            return;
        }
        
        logger.info("Stopping task monitor");
        
        // 停止调度器
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Task monitor stopped");
    }
}