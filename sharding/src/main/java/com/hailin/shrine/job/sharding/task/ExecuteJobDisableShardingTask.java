package com.hailin.shrine.job.sharding.task;

import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExecuteJobDisableShardingTask extends AbstractAsyncShardingTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteJobDisableShardingTask.class);

    private String jobName;

    public ExecuteJobDisableShardingTask(NamespaceShardingService namespaceShardingService, String jobName) {
        super(namespaceShardingService);
        this.jobName = jobName;
    }

    @Override
    protected void logStartInfo() {
        LOGGER.info("Execute the {} with {} disable", this.getClass().getSimpleName(), jobName);
    }

    @Override
    protected boolean pick(List<String> allJobs, List<String> allEnableJobs, List<Shard> shardList, List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception {
        //摘取所有该作业的shard
        shardList.addAll(namespaceShardingService.removeAllShardsOnExecutors(lastOnlineTrafficExecutorList, jobName));
        // 如果shardList为空，则没必要进行放回等操作，摘取失败
        if (shardList.isEmpty()) {
            return false;
        }
        return true;
    }

}
