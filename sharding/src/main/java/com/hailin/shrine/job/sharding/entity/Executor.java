package com.hailin.shrine.job.sharding.entity;

import java.util.List;

public class Executor {

    private String executorName;

    private String ip;

    private boolean noTraffic;

    private List<String> jobNameList;
    private List<Shard> shardList;

    private int totalLoadLevel;

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public boolean isNoTraffic() {
        return noTraffic;
    }

    public void setNoTraffic(boolean noTraffic) {
        this.noTraffic = noTraffic;
    }

    public List<String> getJobNameList() {
        return jobNameList;
    }

    public void setJobNameList(List<String> jobNameList) {
        this.jobNameList = jobNameList;
    }

    public List<Shard> getShardList() {
        return shardList;
    }

    public void setShardList(List<Shard> shardList) {
        this.shardList = shardList;
    }

    public int getTotalLoadLevel() {
        return totalLoadLevel;
    }

    public void setTotalLoadLevel(int totalLoadLevel) {
        this.totalLoadLevel = totalLoadLevel;
    }
}
