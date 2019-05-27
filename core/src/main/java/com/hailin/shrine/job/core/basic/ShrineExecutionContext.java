package com.hailin.shrine.job.core.basic;

import com.hailin.shrine.job.ShrineJobReturn;
import com.hailin.shrine.job.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.shrine.job.core.config.JobConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Shrine的作业运行上下文
 * @author zhanghailin
 */
public class ShrineExecutionContext extends JobExecutionMultipleShardingContext {


    private static int initCollectionSize = 64;

    /**
     * 是否为集成Shrine的作业类型
     */
    private boolean saturnJob = false;

    /**
     * Job超时时间(秒)
     */
    private int timetoutSeconds;

    /**
     * 运行在本作业项的分片序列号和运行结果.
     */
    private Map<Integer, ShrineJobReturn> shardingItemResults = new HashMap<>(initCollectionSize);

    /**
     * 作业运行日志, key为分片项
     */
    private Map<Integer, String> jobLogMap = new HashMap<>();

    /**
     * 作业配置类
     */
    private JobConfiguration jobConfiguration;
    private String namespace;

    private String executorName;

    private Class<?> jobClass;

    public String getJobLog(Integer slice) {
        return jobLogMap.get(slice);
    }

    public void putJobLog(Integer slice, String jobLog) {
        jobLogMap.put(slice, jobLog);
    }

    public static int getInitCollectionSize() {
        return initCollectionSize;
    }

    public static void setInitCollectionSize(int initCollectionSize) {
        ShrineExecutionContext.initCollectionSize = initCollectionSize;
    }

    public boolean isSaturnJob() {
        return saturnJob;
    }

    public void setSaturnJob(boolean saturnJob) {
        this.saturnJob = saturnJob;
    }

    public int getTimetoutSeconds() {
        return timetoutSeconds;
    }

    public void setTimetoutSeconds(int timetoutSeconds) {
        this.timetoutSeconds = timetoutSeconds;
    }

    public Map<Integer, ShrineJobReturn> getShardingItemResults() {
        return shardingItemResults;
    }

    public void setShardingItemResults(Map<Integer, ShrineJobReturn> shardingItemResults) {
        this.shardingItemResults = shardingItemResults;
    }

    public Map<Integer, String> getJobLogMap() {
        return jobLogMap;
    }

    public void setJobLogMap(Map<Integer, String> jobLogMap) {
        this.jobLogMap = jobLogMap;
    }

    public JobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    public void setJobConfiguration(JobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public Class<?> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<?> jobClass) {
        this.jobClass = jobClass;
    }
}
