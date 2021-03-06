package com.hailin.job.schedule.core.executor;

import com.hailin.job.schedule.core.strategy.JobScheduler;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class LimitMaxJobService extends AbstractScheduleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LimitMaxJobService.class);

    public LimitMaxJobService(final JobScheduler jobScheduler ) {
        super(jobScheduler);
    }

    /**
     * 如果当前作业为新增作业，而且超出该域最大作业数量限制，将打印警告日志，返回false; 否则返回true
     * @param jobName 新增作业名
     * @return 是否超出
     */
    public boolean check(String jobName){
        List<String> childrenKeys = coordinatorRegistryCenter.getChildrenKeys(ShrineExecutorsNode.JOBSNODE_PATH);

        if (!CollectionUtils.isEmpty(childrenKeys)
                && !childrenKeys.contains(jobName)
                && childrenKeys.size() >= SystemEnvProperties.SCHEDULE_MAX_NUMBER_OF_JOBS){
            LOGGER.warn(jobName, "The jobs that are under the namespace exceed {}",
                    SystemEnvProperties.SCHEDULE_MAX_NUMBER_OF_JOBS);
            return false;
        }
        return true;
    }
}
