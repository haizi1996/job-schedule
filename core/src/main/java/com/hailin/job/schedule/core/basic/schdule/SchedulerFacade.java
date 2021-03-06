package com.hailin.job.schedule.core.basic.schdule;

import com.hailin.job.schedule.core.basic.election.LeaderElectionService;
import com.hailin.job.schedule.core.basic.execution.ExecutionService;
import com.hailin.job.schedule.core.basic.instance.InstanceService;
import com.hailin.job.schedule.core.basic.reconcile.ReconcileService;
import com.hailin.job.schedule.core.basic.sharding.ShardingService;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.listener.ListenerManager;
import com.hailin.job.schedule.core.schedule.JobTriggerListener;
import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.basic.monitor.MonitorService;
import com.hailin.job.schedule.core.basic.server.ServerService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;

/**
 * 调度器的门面类
 * @author zhanghailin
 */
public final class SchedulerFacade {

    private ConfigurationService configService;

    private LeaderElectionService leaderElectionService;

    private ServerService serverService;

    private ShardingService shardingService;

    private ExecutionService executionService;

    private InstanceService instanceService;

    private final MonitorService monitorService;

    private final ReconcileService reconcileService;

    private ListenerManager listenerManager;

    public SchedulerFacade( final String jobName , final CoordinatorRegistryCenter regCenter) {
        configService = new ConfigurationService(jobName , regCenter);
        leaderElectionService = new LeaderElectionService( jobName , regCenter);
        serverService = new ServerService( jobName , regCenter);
        instanceService = new InstanceService(jobName , regCenter);
        shardingService = new ShardingService(jobName , regCenter);
        executionService = new ExecutionService(jobName , regCenter);
        monitorService = new MonitorService( jobName , regCenter);
        reconcileService = new ReconcileService(regCenter, jobName);
    }
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
        listenerManager.start();
        leaderElectionService.leaderElection();
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

    /**
     * 获取作业触发监听器.
     *
     * @return 作业触发监听器
     */
    public JobTriggerListener newJobTriggerListener() {
        return new JobTriggerListener(executionService, shardingService);
    }
}
