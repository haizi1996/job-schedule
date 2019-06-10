package com.hailin.shrine.job.sharding.task;

import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 作业启用
 */
public class ExecuteJobEnableShardingTask extends AbstractAsyncShardingTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteJobEnableShardingTask.class);
    private String jobName;

    public ExecuteJobEnableShardingTask(NamespaceShardingService namespaceShardingService, String jobName) {
        super(namespaceShardingService);
        this.jobName = jobName;
    }


    @Override
    protected void logStartInfo() {
        LOGGER.info("Execute the {} with {} enable", this.getClass().getSimpleName(), jobName);
    }


    @Override
    protected List<String> notifyEnableJobsPrior() {
        List<String> notifyEnableJobsPrior = new ArrayList<>();
        notifyEnableJobsPrior.add(jobName);
        return notifyEnableJobsPrior;
    }

    @Override
    protected boolean pick(List<String> allJobs, List<String> allEnableJobs, List<Shard> shardList, List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception {
        // 移除已经在Executor运行的该作业的所有Shard
        namespaceShardingService.removeAllShardsOnExecutors(lastOnlineTrafficExecutorList, jobName);

        // 修正该所有executor的对该作业的jobNameList
        fixJobNameList(lastOnlineExecutorList, jobName);

        // 获取该作业的Shard
        shardList.addAll(createShards(jobName, lastOnlineTrafficExecutorList));

        return true;
    }
}
