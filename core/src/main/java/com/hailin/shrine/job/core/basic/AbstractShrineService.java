package com.hailin.shrine.job.core.basic;

import com.hailin.shrine.job.core.basic.storage.JobNodeStorage;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.strategy.JobScheduler;

/**
 * 抽象服务类
 * @author zhanghailin
 */
public class AbstractShrineService implements Shutdownable {

    //执行者名字
    protected String executorName;

    //作业名称
    protected String jobName;

    //作业调度器
    protected JobScheduler jobScheduler;

    protected JobConfiguration jobConfiguration;
    //注册中心
    protected CoordinatorRegistryCenter coordinatorRegistryCenter;

    //作业节点数据访问类
    protected JobNodeStorage jobNodeStorage;

    public AbstractShrineService(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
        this.jobName = jobScheduler.getJobName();
        this.executorName = jobScheduler.getExecutorName();
        this.coordinatorRegistryCenter = jobScheduler.getRegCenter();
        this.jobConfiguration = jobScheduler.getCurrentConf();
        this.jobNodeStorage = jobScheduler.getJobNodeStorage();
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public void setJobScheduler(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    public JobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    public void setJobConfiguration(JobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
    }

    public CoordinatorRegistryCenter getCoordinatorRegistryCenter() {
        return coordinatorRegistryCenter;
    }

    public void setCoordinatorRegistryCenter(CoordinatorRegistryCenter coordinatorRegistryCenter) {
        this.coordinatorRegistryCenter = coordinatorRegistryCenter;
    }

    public JobNodeStorage getJobNodeStorage() {
        return jobNodeStorage;
    }

    public void setJobNodeStorage(JobNodeStorage jobNodeStorage) {
        this.jobNodeStorage = jobNodeStorage;
    }

    public void start(){

    }

    @Override
    public void shutdown() {

    }
}
