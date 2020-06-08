package com.hailin.job.schedule.core.basic.execution;

import java.io.Serializable;

public class ExecutionInfo implements Serializable {

    private int item;

    private String jobMsg;

    private Long lastBeginTime;

    private Long lastCompleteTime;

    private Long nextFireTime;
    //作业分片运行日志
    private String jobLog;

    public ExecutionInfo() {
    }

    public ExecutionInfo(int item) {
        this.item = item;
    }

    public ExecutionInfo(int item, Long lastBeginTime) {
        this.item = item;
        this.lastBeginTime = lastBeginTime;
    }

    public int getItem() {
        return item;
    }

    public void setItem(int item) {
        this.item = item;
    }

    public String getJobMsg() {
        return jobMsg;
    }

    public void setJobMsg(String jobMsg) {
        this.jobMsg = jobMsg;
    }

    public Long getLastBeginTime() {
        return lastBeginTime;
    }

    public void setLastBeginTime(Long lastBeginTime) {
        this.lastBeginTime = lastBeginTime;
    }

    public Long getLastCompleteTime() {
        return lastCompleteTime;
    }

    public void setLastCompleteTime(Long lastCompleteTime) {
        this.lastCompleteTime = lastCompleteTime;
    }

    public Long getNextFireTime() {
        return nextFireTime;
    }

    public void setNextFireTime(Long nextFireTime) {
        this.nextFireTime = nextFireTime;
    }

    public String getJobLog() {
        return jobLog;
    }

    public void setJobLog(String jobLog) {
        this.jobLog = jobLog;
    }

    @Override
    public String toString() {
        return "ExecutionInfo{" +
                "item=" + item +
                ", jobMsg='" + jobMsg + '\'' +
                ", lastBeginTime=" + lastBeginTime +
                ", lastCompleteTime=" + lastCompleteTime +
                ", nextFireTime=" + nextFireTime +
                ", jobLog='" + jobLog + '\'' +
                '}';
    }
}
