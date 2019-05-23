package com.hailin.shrine.job.core.basic.schdule;

import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.config.ConfigurationNode;
import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.listener.ListenerManager;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.basic.storage.JobNodeStorage;
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

    private ListenerManager listenerManager;

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


    /**
     * 注册作业启动信息.
     *
     * @param enabled 作业是否启用
     */
    public void registerStartUpInfo(final boolean enabled) {
        listenerManager.startAllListeners();
        leaderElectionService.electLeader();
        serverService.persistOnline(enabled);
        instanceService.persistOnline();
        shardingService.setReshardingFlag();
//        monitorService.listen();
//        if (!reconcileService.isRunning()) {
//            reconcileService.startAsync();
//        }
    }


    /**
     * 终止作业调度.
     */
    public void shutdownInstance() {
        if (leaderElectionService.isLeader()) {
            leaderElectionService.removeLeader();
        }
//        monitorService.close();
//        if (reconcileService.isRunning()) {
//            reconcileService.stopAsync();
//        }
        JobRegistry.getInstance();
    }
}
