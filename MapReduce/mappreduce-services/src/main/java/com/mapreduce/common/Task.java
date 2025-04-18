package com.mapreduce.common;

import java.io.Serializable;
import java.util.UUID;

/**
 * 表示一个 MapReduce 任务
 */
public class Task implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String taskId;
    private String jobId;
    private TaskType taskType;
    private TaskStatus status;
    private String inputPath;         // 输入文件路径
    private String outputPath;        // 输出文件路径
    private int partitionId;          // 对于 Reduce 任务，表示处理的分区
    private int attemptCount;         // 尝试次数
    private String workerId;          // 处理该任务的 Worker ID
    
    // Blob存储相关字段
    private String inputBlobUrl;      // 输入数据的Blob URL
    private String outputBlobUrl;     // 输出数据的Blob URL
    private StorageType storageType;  // 存储类型
    
    public Task() {
        this.status = TaskStatus.PENDING;
        this.attemptCount = 0;
        this.storageType = StorageType.LOCAL;
    }
    
    /**
     * 创建一个 Map 任务
     */
    public static Task createMapTask(String jobId, String inputPath, String outputPath) {
        Task task = new Task();
        task.taskId = "map_" + UUID.randomUUID().toString().replace("-", "");
        task.jobId = jobId;
        task.taskType = TaskType.MAP;
        task.status = TaskStatus.PENDING;
        task.inputPath = inputPath;
        task.outputPath = outputPath;
        task.attemptCount = 0;
        task.storageType = StorageType.LOCAL;
        return task;
    }
    
    /**
     * 创建一个 Blob存储的Map任务
     */
    public static Task createBlobMapTask(String jobId, String inputPath, String outputPath, 
                                     String inputBlobUrl) {
        Task task = createMapTask(jobId, inputPath, outputPath);
        task.inputBlobUrl = inputBlobUrl;
        task.storageType = StorageType.BLOB;
        return task;
    }
    
    /**
     * 创建一个 Reduce 任务
     */
    public static Task createReduceTask(String jobId, int partitionId, String outputPath) {
        Task task = new Task();
        task.taskId = "reduce_" + UUID.randomUUID().toString().replace("-", "");
        task.jobId = jobId;
        task.taskType = TaskType.REDUCE;
        task.status = TaskStatus.PENDING;
        task.partitionId = partitionId;
        task.outputPath = outputPath;
        task.attemptCount = 0;
        task.storageType = StorageType.LOCAL;
        return task;
    }
    
    /**
     * 创建一个 Blob存储的Reduce任务
     */
    public static Task createBlobReduceTask(String jobId, int partitionId, String outputPath) {
        Task task = createReduceTask(jobId, partitionId, outputPath);
        task.setStorageType(StorageType.BLOB);
        return task;
    }
    
    // Getters and Setters
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getJobId() {
        return jobId;
    }
    
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
    
    public TaskType getTaskType() {
        return taskType;
    }
    
    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }
    
    public TaskStatus getStatus() {
        return status;
    }
    
    public void setStatus(TaskStatus status) {
        this.status = status;
    }
    
    public String getInputPath() {
        return inputPath;
    }
    
    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }
    
    public String getOutputPath() {
        return outputPath;
    }
    
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    
    public int getPartitionId() {
        return partitionId;
    }
    
    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }
    
    public int getAttemptCount() {
        return attemptCount;
    }
    
    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }
    
    public void incrementAttemptCount() {
        this.attemptCount++;
    }
    
    public String getWorkerId() {
        return workerId;
    }
    
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
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
    
    public StorageType getStorageType() {
        return storageType;
    }
    
    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }
    
    @Override
    public String toString() {
        return "Task{" +
                "taskId='" + taskId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", taskType=" + taskType +
                ", status=" + status +
                ", partitionId=" + partitionId +
                ", attemptCount=" + attemptCount +
                ", storageType=" + storageType +
                '}';
    }
}