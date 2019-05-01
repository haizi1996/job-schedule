package com.hailin.shrine.job.core.strategy;

import com.hailin.hjob.common.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;

/**
 * 作业调度器
 * @author zhanghailin
 */
public class JobScheduler {

    public static final String JOB_DATA_MAP_KEY = "hJob";

    private static final String JOB_FACADE_DATA_MAP_KEY = "jobFacade";

    private final JobConfiguration jobConfig;

    private final CoordinatorRegistryCenter regCenter;

    public JobScheduler(JobConfiguration jobConfig, CoordinatorRegistryCenter regCenter) {
        this.jobConfig = jobConfig;
        this.regCenter = regCenter;
    }
}
