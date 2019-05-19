package com.hailin.shrine.job.core.basic.execution;

import com.google.common.collect.Lists;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.ShrineExecutionContext;
import com.hailin.shrine.job.core.basic.failover.FailoverService;
import com.hailin.shrine.job.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.shrine.job.core.job.trigger.Triggered;
import com.hailin.shrine.job.core.service.ConfigurationService;
import com.hailin.shrine.job.core.strategy.JobScheduler;

import java.util.List;

/**
 * 作业运行时上下文服务
 * @author zhanghailin
 */
public class ExecutionContextService extends AbstractShrineService {

    private ConfigurationService configurationService;

    private FailoverService failoverService;

    public ExecutionContextService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        configurationService = jobScheduler.getConfigService();
//        failoverService =
    }


    public JobExecutionMultipleShardingContext getJobExecutionMultipleShardingContext(final Triggered triggered){
        ShrineExecutionContext result = new ShrineExecutionContext();
        result.setJobName(configurationService.getJobName());
        result.setShardingTotalCount(configurationService.getShardingTotalCount());
        result.setTriggered(triggered);
        List<Integer> shardingItems = getShardingItems();
        boolean isEnabledReport = configurationService.isEnabledReport();
        if(isEnabledReport){
            removeRunningItems(shardingItems);
        }
        result.setShardingItems(shardingItems);
        result.setjob

    }

    private void removeRunningItems(final List<Integer> items){
        List<Integer> toBeRemoveItems = Lists.newArrayList();
        for (int each:  items  ) {
            if(isRunningItem(each)){
                toBeRemoveItems.add(each);
            }
        }
        items.removeAll(toBeRemoveItems);
    }

    private boolean isRunningItem(final int item){
        return jobScheduler.getJobNodeStorage().isJobNodeExisted(ExecutionNode.getRunningNode(item));
    }

    /**
     * 获取分片项列表
     * @return 分片项列表
     */
    public List<Integer> getShardingItems(){

//        List<Integer> shardingItems = jobScheduler.gets
        boolean isEnabledReport = configurationService.isEnabledReport();
        if (configurationService.i)

        return Lists.newArrayList();
    }



}
