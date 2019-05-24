package com.hailin.shrine.job.core.schedule;


import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ShrineJob implements Job {

    private JobScheduler jobScheduler;

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public void setJobScheduler(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        //调度任务
        //从dataMap 获取调度器 进行任务调度执行
    }
}
