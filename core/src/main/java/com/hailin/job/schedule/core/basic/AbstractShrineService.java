package com.hailin.job.schedule.core.basic;

import com.hailin.job.schedule.core.basic.storage.JobNodeStorage;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import lombok.Getter;
import lombok.Setter;

/**
 * 抽象服务类
 * @author zhanghailin
 */
@Getter
@Setter
public abstract class AbstractShrineService implements Shutdownable {

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

    public AbstractShrineService(String jobName, CoordinatorRegistryCenter coordinatorRegistryCenter) {
        this.jobName = jobName;
        this.coordinatorRegistryCenter = coordinatorRegistryCenter;
        this.jobNodeStorage = new JobNodeStorage(coordinatorRegistryCenter , jobName);
    }

//    public AbstractShrineService(JobScheduler jobScheduler) {
//        this.jobScheduler = jobScheduler;
//        this.jobName = jobScheduler.getJobName();
//        this.executorName = jobScheduler.getExecutorName();
//        this.coordinatorRegistryCenter = jobScheduler.getRegCenter();
//        this.jobConfiguration = jobScheduler.getCurrentConf();
//        this.jobNodeStorage = jobScheduler.getJobNodeStorage();
//    }



    public void start(){

    }

    @Override
    public void shutdown() {

    }
}
