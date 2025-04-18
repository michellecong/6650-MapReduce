package com.mapreduce.common;

/**
 * 任务状态枚举
 */
public enum TaskStatus {
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
