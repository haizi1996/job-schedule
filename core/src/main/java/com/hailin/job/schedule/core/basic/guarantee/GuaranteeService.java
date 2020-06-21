
package com.hailin.job.schedule.core.basic.guarantee;


import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;

import java.util.Collection;

/**
 * 保证分布式任务全部开始和结束状态的服务.
 * 
 */
public final class GuaranteeService extends AbstractScheduleService {
    

    private final ConfigurationService configService;
    
    public GuaranteeService(final String jobName , final CoordinatorRegistryCenter regCenter) {
        super(jobName ,regCenter);
        configService = new ConfigurationService( jobName , regCenter);
    }
    
    /**
     * 根据分片项注册任务开始运行.
     * 
     * @param shardingItems 待注册的分片项
     */
    public void registerStart(final Collection<Integer> shardingItems) {
        for (int each : shardingItems) {
            jobNodeStorage.createJobNodeIfNeeded(GuaranteeNode.getStartedNode(each));
        }
    }
    
    /**
     * 判断是否所有的任务均启动完毕.
     *
     * @return 是否所有的任务均启动完毕
     */
    public boolean isAllStarted() {
        return jobNodeStorage.isJobNodeExisted(GuaranteeNode.STARTED_ROOT)
                && configService.load(false).getTypeConfig().getCoreConfig().getShardingTotalCount() == jobNodeStorage.getJobNodeChildrenKeys(GuaranteeNode.STARTED_ROOT).size();
    }
    
    /**
     * 清理所有任务启动信息.
     */
    public void clearAllStartedInfo() {
        jobNodeStorage.removeJobNodeIfExisted(GuaranteeNode.STARTED_ROOT);
    }
    
    /**
     * 根据分片项注册任务完成运行.
     *
     * @param shardingItems 待注册的分片项
     */
    public void registerComplete(final Collection<Integer> shardingItems) {
        for (int each : shardingItems) {
            jobNodeStorage.createJobNodeIfNeeded(GuaranteeNode.getCompletedNode(each));
        }
    }
    
    /**
     * 判断是否所有的任务均执行完毕.
     *
     * @return 是否所有的任务均执行完毕
     */
    public boolean isAllCompleted() {
        return jobNodeStorage.isJobNodeExisted(GuaranteeNode.COMPLETED_ROOT)
                && configService.load(false).getTypeConfig().getCoreConfig().getShardingTotalCount() <= jobNodeStorage.getJobNodeChildrenKeys(GuaranteeNode.COMPLETED_ROOT).size();
    }
    
    /**
     * 清理所有任务启动信息.
     */
    public void clearAllCompletedInfo() {
        jobNodeStorage.removeJobNodeIfExisted(GuaranteeNode.COMPLETED_ROOT);
    }
}
