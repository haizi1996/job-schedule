package com.hailin.job.schedule.core.basic.monitor;

import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;

/**
 * 作业监控服务
 * @author zhanghailin
 */
public class MonitorService extends AbstractScheduleService {


    private final ConfigurationService configurationService;

    private volatile boolean closed;

    public MonitorService(String jobName, CoordinatorRegistryCenter coordinatorRegistryCenter) {
        super(jobName, coordinatorRegistryCenter);
        configurationService = new ConfigurationService(jobName , coordinatorRegistryCenter);
    }

    /**
     * 初始化作业监听服务
     */
    public void listener(){
    }
}
