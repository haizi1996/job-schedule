package com.hailin.shrine.job.sharding.task;

import com.hailin.shrine.job.sharding.service.NamespaceShardingService;

public class ExecuteAllShardingTask extends AbstractAsyncShardingTask {

    public ExecuteAllShardingTask(NamespaceShardingService namespaceShardingService) {
        super(namespaceShardingService);
    }
}
