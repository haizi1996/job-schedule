package com.hailin.job.schedule.core.basic.execution;

import com.google.common.collect.Lists;
import com.hailin.job.schedule.core.basic.ScheduleExecutionContext;
import com.hailin.job.schedule.core.basic.failover.FailoverService;
import com.hailin.job.schedule.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.strategy.JobScheduler;

import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * 作业运行时上下文服务
 * @author zhanghailin
 */
public class ExecutionContextService extends AbstractScheduleService {

    private ConfigurationService configService;

    private FailoverService failoverService;


    public ExecutionContextService(final JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        configService = jobScheduler.getConfigService();
        failoverService = jobScheduler.getFailoverService();
    }


    private boolean isRunningItem(final int shardingItem) {
        return jobScheduler.getJobNodeStorage().isJobNodeExisted(ExecutionNode.getRunningNode(shardingItem));
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
     * 获取分片项列表。
     * @return 分片项列表。
     */
    public List<Integer> getShardingItems() {
        List<Integer> shardingItems = jobScheduler.getShardingService().getLocalHostShardingItems();
        boolean isEnabledReport = configService.isEnabledReport();
        if (configService.isFailover() && isEnabledReport) {
            List<Integer> failoverItems = failoverService.getLocalHostFailoverItems();
            if (!failoverItems.isEmpty()) {
                return failoverItems;
            } else {
                shardingItems.removeAll(failoverService.getLocalHostTakeOffItems());
                return shardingItems;
            }
        } else {
            return shardingItems;
        }
    }
}
