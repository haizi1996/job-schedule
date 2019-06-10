package com.hailin.shrine.job.sharding.task;

import com.hailin.shrine.job.sharding.entity.Executor;
import com.hailin.shrine.job.sharding.entity.Shard;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * 作业的executor下线，将该executor运行的该作业分片都摘取，如果是本地作业，则移除
 */
public class ExecuteJobServerOfflineShardingTask extends AbstractAsyncShardingTask {
    private static final Logger log = LoggerFactory.getLogger(ExecuteJobServerOfflineShardingTask.class);

    private String jobName;

    private String executorName;

    public ExecuteJobServerOfflineShardingTask(NamespaceShardingService namespaceShardingService, String jobName, String executorName) {
        super(namespaceShardingService);
        this.jobName = jobName;
        this.executorName = executorName;
    }

    @Override
    protected void logStartInfo() {
        log.info("Execute the {}, jobName is {}, executorName is {}", this.getClass().getSimpleName(), jobName,
                executorName);
    }

    @Override
    protected boolean pick(List<String> allJobs, List<String> allEnableJobs, List<Shard> shardList, List<Executor> lastOnlineExecutorList, List<Executor> lastOnlineTrafficExecutorList) throws Exception {
        boolean localMode = isLocalMode(jobName);
        for (Executor executor : lastOnlineExecutorList) {
            if (!executor.getExecutorName().equals(executorName)){
                continue;
            }
            Iterator<Shard> iterator = executor.getShardList().iterator();
            while (iterator.hasNext()) {
                Shard shard = iterator.next();
                if (shard.getJobName().equals(jobName)) {
                    if (!localMode) {
                        shardList.add(shard);
                    }
                    iterator.remove();
                }
            }
            executor.getJobNameList().remove(jobName);
            break;
        }

        return true;
    }
}
