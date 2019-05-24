
package com.hailin.shrine.job.core.basic.reconcile;


import com.google.common.util.concurrent.AbstractScheduledService;
import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.service.ConfigurationService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * 调解分布式作业不一致状态服务.
 *
 * @author caohao
 */
@Slf4j
public final class ReconcileService extends AbstractScheduledService {
    
    private long lastReconcileTime;
    
    private final ConfigurationService configService;
    
    private final ShardingService shardingService;
    
    private final LeaderElectionService leaderService;
    
    public ReconcileService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        lastReconcileTime = System.currentTimeMillis();
        configService = new ConfigurationService(jobName , regCenter);
        shardingService = new ShardingService(jobName , regCenter);
        leaderService = new LeaderElectionService(jobName , regCenter);
    }
    
    @Override
    protected void runOneIteration() throws Exception {
        JobConfiguration config = configService.load(true);
        int reconcileIntervalMinutes = null == config ? -1 : config.getReconcileIntervalMinutes();
        if (reconcileIntervalMinutes > 0 && (System.currentTimeMillis() - lastReconcileTime >= reconcileIntervalMinutes * 60 * 1000)) {
            lastReconcileTime = System.currentTimeMillis();
            if (leaderService.isLeaderUntilBlock() && !shardingService.isNeedSharding() && shardingService.hasShardingInfoInOfflineServers()) {
                log.warn("Elastic Job: job status node has inconsistent value,start reconciling...");
                shardingService.setReshardingFlag();
            }
        }
    }
    
    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, 1, TimeUnit.MINUTES);
    }
}
