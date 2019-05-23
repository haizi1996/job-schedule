package com.hailin.shrine.job.core.schedule;

import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import lombok.RequiredArgsConstructor;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;

/**
 * 作业触发监听器
 * @author zhanghailin
 */
@RequiredArgsConstructor
public class JobTriggerListener extends TriggerListenerSupport {


    private final ExecutionService executionService;

    private final ShardingService shardingService;


    @Override
    public String getName() {
        return "JobTriggerListener";
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
        if (null != trigger.getPreviousFireTime()){
            executionService.setMisfire(shardingService.getLocalShardingItems());
        }
    }
}
