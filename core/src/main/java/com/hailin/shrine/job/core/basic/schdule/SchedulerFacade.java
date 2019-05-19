package com.hailin.shrine.job.core.basic.schdule;

import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.service.ConfigurationService;

/**
 * 调度器的门面类
 * @author zhanghailin
 */
public final class SchedulerFacade {

    private String jobName;

    private ConfigurationService configurationService;

    private LeaderElectionService leaderElectionService;

    private ServerService serverService;

    private ShardingService shardingService;

    private ExecutionService executionService;


}
