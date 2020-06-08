
package com.hailin.job.schedule.core.basic.schdule;

import com.hailin.job.schedule.core.basic.execution.ExecutionContextService;
import com.hailin.job.schedule.core.basic.execution.ExecutionService;
import com.hailin.job.schedule.core.basic.execution.ShardingContexts;
import com.hailin.job.schedule.core.basic.failover.FailoverService;
import com.hailin.job.schedule.core.basic.sharding.ShardingService;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.config.dataflow.DataflowJobConfiguration;
import com.hailin.job.schedule.core.executor.ShardingContext;
import com.hailin.job.schedule.core.job.JobFacade;
import com.hailin.job.schedule.core.listener.ElasticJobListener;
import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.shrine.job.common.event.JobExecutionEvent;
import com.hailin.shrine.job.common.event.JobStatusTraceEvent;
import com.hailin.shrine.job.common.exception.JobExecutionEnvironmentException;
import com.hailin.shrine.job.common.exception.JobShuttingDownException;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;

/**
 * 为作业提供内部服务的门面类.
 * 
 * @author zhanghailin
 */
@Slf4j
public final class ShrineJobFacade implements JobFacade {
    
    private final ConfigurationService configService;
    
    private final ShardingService shardingService;
    
    private final ExecutionContextService executionContextService;
    
    private final ExecutionService executionService;
    
    private final FailoverService failoverService;
    
    private List<ElasticJobListener> elasticJobListeners;
    

    public ShrineJobFacade( final String jobName , final CoordinatorRegistryCenter regCenter) {
        configService = new ConfigurationService( jobName , regCenter);
        shardingService = new ShardingService(jobName , regCenter);
        executionContextService = new ExecutionContextService(jobName , regCenter);
        executionService = new ExecutionService(jobName , regCenter);
        failoverService = new FailoverService(jobName , regCenter);
    }

    @Override
    public void beforeJobExecuted(ShardingContext shardingContext) {

    }

    @Override
    public void afterJobExecuted(ShardingContext shardingContext) {

    }

    @Override
    public JobConfiguration loadJobRootConfiguration(final boolean fromCache) {
        return configService.load(fromCache);
    }
    
    @Override
    public void checkJobExecutionEnvironment() throws JobExecutionEnvironmentException {
        configService.checkMaxTimeDiffSecondsTolerable();
    }
    
    @Override
    public void failoverIfNecessary() {
        if (configService.load(true).isFailover()) {
            failoverService.failoverIfNecessary();
        }
    }
    
    @Override
    public void registerJobBegin(final ShardingContexts shardingContexts) {
        executionService.registerJobBegin(shardingContexts);
    }
    
    @Override
    public void registerJobCompleted(final ShardingContexts shardingContexts) {
        executionService.registerJobCompleted(shardingContexts);
        if (configService.load(true).isFailover()) {
            failoverService.updateFailoverComplete(shardingContexts.getShardingItemParameters().keySet());
        }
    }
    
    @Override
    public ShardingContexts getShardingContexts() {
        boolean isFailover = configService.load(true).isFailover();
        if (isFailover) {
            List<Integer> failoverShardingItems = failoverService.getLocalHostFailoverItems();
            if (!failoverShardingItems.isEmpty()) {
                return executionContextService.getJobShardingContext(failoverShardingItems);
            }
        }
        try {
            shardingService.shardingIfNecessary();
        } catch (JobShuttingDownException e) {
            log.debug("" , e);
        }
        List<Integer> shardingItems = shardingService.getLocalShardingItems();
        if (isFailover) {
            shardingItems.removeAll(failoverService.getLocalTakeOffItems());
        }
        shardingItems.removeAll(executionService.getDisabledItems(shardingItems));
        return executionContextService.getJobShardingContext(shardingItems);
    }
    
    @Override
    public boolean misfireIfRunning(final Collection<Integer> shardingItems) {
        return executionService.misfireIfHasRunningItems(shardingItems);
    }
    
    @Override
    public void clearMisfire(final Collection<Integer> shardingItems) {
        executionService.clearMisfire(shardingItems);
    }
    
    @Override
    public boolean isExecuteMisfired(final Collection<Integer> shardingItems) {
        return isEligibleForJobRunning() && configService.load(true).getTypeConfig().getCoreConfig().isMisfire() && !executionService.getMisfiredJobItems(shardingItems).isEmpty();
    }
    
    @Override
    public boolean isEligibleForJobRunning() {
        JobConfiguration liteJobConfig = configService.load(true);
        if (liteJobConfig.getTypeConfig() instanceof DataflowJobConfiguration) {
            return !shardingService.isNeedSharding() && ((DataflowJobConfiguration) liteJobConfig.getTypeConfig()).isStreamingProcess();
        }
        return !shardingService.isNeedSharding();
    }
    
    @Override
    public boolean isNeedSharding() {
        return shardingService.isNeedSharding();
    }
    

    
    @Override
    public void postJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
//        jobEventBus.post(jobExecutionEvent);
    }
    
    @Override
    public void postJobStatusTraceEvent(final String taskId, final JobStatusTraceEvent.State state, final String message) {
//        TaskContext taskContext = TaskContext.from(taskId);
//        jobEventBus.post(new JobStatusTraceEvent(taskContext.getMetaInfo().getJobName(), taskContext.getId(),
//                taskContext.getSlaveId(), JobStatusTraceEvent.Source.LITE_EXECUTOR, taskContext.getType(), taskContext.getMetaInfo().getShardingItems().toString(), state, message));
//        if (!Strings.isNullOrEmpty(message)) {
//            log.trace(message);
//        }
    }
}
