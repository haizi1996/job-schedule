package com.hailin.shrine.job.core.basic.schdule;

import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.service.ConfigurationService;

/**
 * 调度器的门面类
 * @author zhanghailin
 */
public final class SchedulerFacade {

    private String jobName;

    private ConfigurationService configService;

    private LeaderElectionService leaderElectionService;

    private ServerService serverService;

    private ShardingService shardingService;

    private ExecutionService executionService;

    /**
     * 更新作业配置.
     *
     * @param jobConfiguration 作业配置
     * @return 更新后的作业配置
     */
    public JobConfiguration updateJobConfiguration(final JobConfiguration jobConfiguration) {
        configService.persist(jobConfiguration);
        return configService.load(false);
    }

}
