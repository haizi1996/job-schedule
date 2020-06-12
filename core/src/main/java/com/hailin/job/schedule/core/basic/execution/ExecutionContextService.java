package com.hailin.job.schedule.core.basic.execution;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.hailin.job.schedule.core.basic.ScheduleExecutionContext;
import com.hailin.job.schedule.core.basic.failover.FailoverService;
import com.hailin.job.schedule.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.strategy.JobInstance;
import com.hailin.job.schedule.core.basic.AbstractShrineService;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.basic.sharding.ShardingNode;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;

import java.util.*;

/**
 * 作业运行时上下文服务
 * @author zhanghailin
 */
public class ExecutionContextService extends AbstractShrineService {

    private ConfigurationService configService;

    private FailoverService failoverService;


    public ExecutionContextService(String jobName, CoordinatorRegistryCenter coordinatorRegistryCenter) {
        super(jobName, coordinatorRegistryCenter);
        this.configService = new ConfigurationService(jobName , coordinatorRegistryCenter);
    }

    @Override
    public void start() {
        configService = jobScheduler.getConfigService();
        failoverService = jobScheduler.getFailoverService();
    }


    /**
     * 获取当前作业服务器分片上下文.
     *
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    public ShardingContexts getJobShardingContext(final List<Integer> shardingItems) {
        JobConfiguration liteJobConfig = configService.load(false);
        removeRunningIfMonitorExecution(liteJobConfig.isMonitorExecution(), shardingItems);
        if (shardingItems.isEmpty()) {
            return new ShardingContexts(buildTaskId(liteJobConfig, shardingItems), liteJobConfig.getJobName(), liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(),
                    liteJobConfig.getTypeConfig().getCoreConfig().getJobParameter(), Collections.<Integer, String>emptyMap());
        }
        Map<Integer, String> shardingItemParameterMap = new ShardingItemParameters(liteJobConfig.getTypeConfig().getCoreConfig().getShardingItemParameters()).getMap();
        return new ShardingContexts(buildTaskId(liteJobConfig, shardingItems), liteJobConfig.getJobName(), liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(),
                liteJobConfig.getTypeConfig().getCoreConfig().getJobParameter(), getAssignedShardingItemParameterMap(shardingItems, shardingItemParameterMap));
    }




    private String buildTaskId(final JobConfiguration liteJobConfig, final List<Integer> shardingItems) {
        JobInstance jobInstance = JobRegistry.getInstance().getJobInstance(jobName);
        return Joiner.on("@-@").join(liteJobConfig.getJobName(), Joiner.on(",").join(shardingItems), "READY",
                null == jobInstance.getJobInstanceId() ? "127.0.0.1@-@1" : jobInstance.getJobInstanceId());
    }

    private boolean isRunningItem(final int shardingItem) {
        return jobScheduler.getJobNodeStorage().isJobNodeExisted(ExecutionNode.getRunningNode(shardingItem));
    }

    private Map<Integer, String> getAssignedShardingItemParameterMap(final List<Integer> shardingItems, final Map<Integer, String> shardingItemParameterMap) {
        Map<Integer, String> result = new HashMap<>(shardingItemParameterMap.size(), 1);
        for (int each : shardingItems) {
            result.put(each, shardingItemParameterMap.get(each));
        }
        return result;
    }
    private void removeRunningIfMonitorExecution(final boolean monitorExecution, final List<Integer> shardingItems) {
        if (!monitorExecution) {
            return;
        }
        List<Integer> runningShardingItems = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (isRunningItem(each)) {
                runningShardingItems.add(each);
            }
        }
        shardingItems.removeAll(runningShardingItems);
    }

    public JobExecutionMultipleShardingContext getJobExecutionShardingContext(){
        ScheduleExecutionContext result = new ScheduleExecutionContext();
        result.setJobName(configService.getJobName());
        result.setShardingTotalCount(configService.getShardingTotalCount());
        result.setTriggered(null);

        List<Integer> shardingItems = getShardingItems();
        boolean isEnabledReport = configService.isEnabledReport();
        if (isEnabledReport){
            removeRunningItems(shardingItems);
        }

        result.setShardingItems(shardingItems);
        result.setJobParameter(configService.getJobParameter());
        result.setCustomContext(configService.getCustomContext());
        result.setJobConfiguration(jobConfiguration);
        if (Objects.nonNull(coordinatorRegistryCenter)){
            result.setNamespace(coordinatorRegistryCenter.getNamespace());
            result.setExecutorName(coordinatorRegistryCenter.getExecutorName());;
        }
        if (result.getShardingItems().isEmpty()){
            return result;
        }
        Map<Integer , String> shardingItemParameters = configService.getShardingItemParameters();
        if (shardingItemParameters.containsKey(-1)){ // 本地模式
            for (int each : result.getShardingItems()) {
                result.getShardingItemParameters().put(each , shardingItemParameters.get(-1));
            }
        }else {
            for (int each : result.getShardingItems()) {
                if (shardingItemParameters.containsKey(each)) {
                    result.getShardingItemParameters().put(each, shardingItemParameters.get(each));
                }
            }
        }

        if (jobConfiguration.getTimeoutSeconds() > 0){
            result.setTimetoutSeconds(jobConfiguration.getTimeoutSeconds());
        }

        return result;
    }

    private void removeRunningItems(List<Integer> shardingItems) {
        List<Integer> toBeRemovedItems = Lists.newArrayListWithCapacity(shardingItems.size());
        for (int item : shardingItems  ) {
            if (isRunningItem(item)){
                toBeRemovedItems.add(item);
            }
        }
        shardingItems.removeAll(toBeRemovedItems);
    }

    /**
     * 获取分片列表
     * @return 分片项列表
     */
    private List<Integer> getShardingItems() {
        boolean isEnableReport = configService.isEnabledReport();
        List<Integer> shardingItems = jobScheduler.getShardingService().getLocalHostShardingItems();
        if (configService.isFailover() && isEnableReport){
            List<Integer> failoverItems = failoverService.getLocalHostFailoverItems();
            if (! failoverItems.isEmpty()){
                return failoverItems;
            }else {
                shardingItems.removeAll(failoverService.getLocalTakeOffItems());
                return shardingItems;
            }


        }else {
            return shardingItems;
        }
    }


}
