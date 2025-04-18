package com.mapreduce.web.model;

import java.sql.Timestamp;

/**
 * Job model class representing a MapReduce job
 */
public class Job {
    // Job status enum
    public enum Status {
        PENDING, RUNNING, COMPLETED, FAILED
    }
    
    // Storage type enum
    public enum StorageType {
        LOCAL, BLOB
    }
    
    private String jobId;
    private Status status;
    private String inputFile;
    private int numMapTasks;
    private int numReduceTasks;
    private Timestamp createdTime;
    private Timestamp startTime;
    private Timestamp finishTime;
    private String inputBlobUrl;
    private String outputBlobUrl;
    private StorageType storageType;
    
    // Default constructor
    public Job() {
        this.status = Status.PENDING;
        this.numReduceTasks = 5;
        this.storageType = StorageType.BLOB;
    }
    
    // Constructor with parameters
    public Job(String jobId, String inputFile, int numReduceTasks, StorageType storageType, String inputBlobUrl) {
        this.jobId = jobId;
        this.inputFile = inputFile;
        this.numReduceTasks = numReduceTasks;
        this.status = Status.PENDING;
        this.storageType = storageType;
        this.inputBlobUrl = inputBlobUrl;
    }
    
    // Getters and setters
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
    
    public String getInputFile() {
        return inputFile;
    }
    
    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }
    
    public int getNumMapTasks() {
        return numMapTasks;
    }
    
    public void setNumMapTasks(int numMapTasks) {
        this.numMapTasks = numMapTasks;
    }
    
    public int getNumReduceTasks() {
        return numReduceTasks;
    }
    
    public void setNumReduceTasks(int numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }
    
    public Timestamp getCreatedTime() {
        return createdTime;
    }
    
    public void setCreatedTime(Timestamp createdTime) {
        this.createdTime = createdTime;
    }
    
    public Timestamp getStartTime() {
        return startTime;
    }
    
    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }
    
    public Timestamp getFinishTime() {
        return finishTime;
    }
    
    public void setFinishTime(Timestamp finishTime) {
        this.finishTime = finishTime;
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
        return "Job{" +
                "jobId='" + jobId + '\'' +
                ", status=" + status +
                ", inputFile='" + inputFile + '\'' +
                ", numMapTasks=" + numMapTasks +
                ", numReduceTasks=" + numReduceTasks +
                ", storageType=" + storageType +
                '}';
    }
}