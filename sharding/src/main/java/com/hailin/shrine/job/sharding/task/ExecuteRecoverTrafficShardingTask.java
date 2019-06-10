package com.hailin.shrine.job.sharding.task;

import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * 恢复executor流量，标记该executor的noTraffic为false，平衡摘取分片
 */
public class ExecuteRecoverTrafficShardingTask extends AbstractAsyncShardingTask{

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteRecoverTrafficShardingTask.class);

    private String executorName;

    public ExecuteRecoverTrafficShardingTask(NamespaceShardingService namespaceShardingService, String executorName) {
        super(namespaceShardingService);
        this.executorName = executorName;
    }
    @Override
    protected void logStartInfo() {
        LOGGER.info("Execute the {} with {} recover traffic", this.getClass().getSimpleName(), executorName);
    }

    @Override
    protected boolean pick(List<String> allJobs, List<String> allEnableJobs, List<Shard> shardList, List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception {
        Executor targetExecutor = null;
        Iterator<Executor> iterator = lastOnlineExecutorList.iterator();
        while (iterator.hasNext()){
            Executor executor = iterator.next();
            if (executor.getExecutorName().equals(executorName)){
                executor.setNoTraffic(false);
                lastOnlineTrafficExecutorList.add(executor);
                targetExecutor = executor;
                break;
            }
        }
        if (targetExecutor == null) {
            LOGGER.warn("The executor {} maybe offline, unnecessary to recover traffic", executorName);
            return false;
        }

        // 平衡摘取每个作业能够运行的分片，可以视为jobNameList中每个作业的jobServerOnline
        final List<String> jobNameList = targetExecutor.getJobNameList();
        for (String jobName : jobNameList) {
            new ExecuteJobServerOnlineShardingTask(namespaceShardingService, jobName, executorName)
                    .pickIntelligent(allEnableJobs, shardList, lastOnlineTrafficExecutorList);
        }

        return true;
    }
}
