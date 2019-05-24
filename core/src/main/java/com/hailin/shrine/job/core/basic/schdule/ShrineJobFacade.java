
package com.hailin.shrine.job.core.basic.schdule;

import com.google.common.base.Strings;
import com.hailin.shrine.job.common.event.JobExecutionEvent;
import com.hailin.shrine.job.common.event.JobStatusTraceEvent;
import com.hailin.shrine.job.common.exception.JobExecutionEnvironmentException;
import com.hailin.shrine.job.core.basic.execution.ExecutionContextService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.failover.FailoverService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.basic.sharding.context.AbstractJobExecutionShardingContext;
import com.hailin.shrine.job.core.job.JobFacade;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.service.ConfigurationService;
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
    
    private final List<ElasticJobListener> elasticJobListeners;
    

    public ShrineJobFacade(final CoordinatorRegistryCenter regCenter, final String jobName) {
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(jobName , regCenter);
        executionContextService = new ExecutionContextService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        failoverService = new FailoverService(regCenter, jobName);
        this.elasticJobListeners = elasticJobListeners;
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
    public void registerJobBegin(final AbstractJobExecutionShardingContext shardingContexts) {
        executionService.registerJobBegin(shardingContexts);
    }
    
    @Override
    public void registerJobCompleted(final AbstractJobExecutionShardingContext shardingContexts) {
        executionService.registerJobCompleted(shardingContexts);
        if (configService.load(true).isFailover()) {
            failoverService.updateFailoverComplete(shardingContexts.getShardingItemParameters().keySet());
        }
    }
    
    @Override
    public AbstractJobExecutionShardingContext getShardingContexts() {
        boolean isFailover = configService.load(true).isFailover();
        if (isFailover) {
            List<Integer> failoverShardingItems = failoverService.getLocalFailoverItems();
            if (!failoverShardingItems.isEmpty()) {
                return executionContextService.getJobShardingContext(failoverShardingItems);
            }
        }
        shardingService.shardingIfNecessary();
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
    public void beforeJobExecuted(final AbstractJobExecutionShardingContext shardingContexts) {
        for (ElasticJobListener each : elasticJobListeners) {
            each.beforeJobExecuted(shardingContexts);
        }
    }
    
    @Override
    public void afterJobExecuted(final AbstractJobExecutionShardingContext shardingContexts) {
        for (ElasticJobListener each : elasticJobListeners) {
            each.afterJobExecuted(shardingContexts);
        }
    }
    
    @Override
    public void postJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        jobEventBus.post(jobExecutionEvent);
    }
    
    @Override
    public void postJobStatusTraceEvent(final String taskId, final JobStatusTraceEvent.State state, final String message) {
        TaskContext taskContext = TaskContext.from(taskId);
        jobEventBus.post(new JobStatusTraceEvent(taskContext.getMetaInfo().getJobName(), taskContext.getId(),
                taskContext.getSlaveId(), JobStatusTraceEvent.Source.LITE_EXECUTOR, taskContext.getType(), taskContext.getMetaInfo().getShardingItems().toString(), state, message));
        if (!Strings.isNullOrEmpty(message)) {
            log.trace(message);
        }
    }
}
