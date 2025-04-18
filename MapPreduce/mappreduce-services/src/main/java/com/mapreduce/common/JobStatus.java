package com.mapreduce.common;

/**
 * 作业状态枚举
 */
public enum JobStatus {
    /**
     * 等待执行
     */
    PENDING,
    
    /**
     * 正在执行
     */
    RUNNING,
    
    /**
     * 已完成
     */
    COMPLETED,
    
    /**
     * 执行失败
     */
    FAILED
}
