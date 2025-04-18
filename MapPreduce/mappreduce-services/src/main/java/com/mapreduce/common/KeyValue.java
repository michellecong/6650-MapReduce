package com.mapreduce.common;

import java.io.Serializable;

/**
 * 表示键值对
 */
public class KeyValue implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String key;
    private int value;
    
    public KeyValue() {
    }
    
    public KeyValue(String key, int value) {
        this.key = key;
        this.value = value;
    }
    
    public String getKey() {
        return key;
    }
    
    public void setKey(String key) {
        this.key = key;
    }
    
    public int getValue() {
        return value;
    }
    
    public void setValue(int value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return key + "\t" + value;
    }
}
