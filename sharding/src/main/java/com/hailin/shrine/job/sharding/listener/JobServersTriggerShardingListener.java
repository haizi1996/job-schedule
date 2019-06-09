package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

@AllArgsConstructor
public class JobServersTriggerShardingListener extends AbstractTreeCacheListener {

    private String jobName;
    private NamespaceShardingService namespaceShardingService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        if (isJobServerStatusAddedOrRemoved(type, path)) {
            String executorName = ShrineExecutorsNode.getJobServersExecutorNameByStatusPath(path);
            if (type == TreeCacheEvent.Type.NODE_ADDED) {
                namespaceShardingService.asyncShardingWhenJobServerOnline(jobName, executorName);
            } else {
                namespaceShardingService.asyncShardingWhenJobServerOffline(jobName, executorName);
            }
        }
    }
    private boolean isJobServerStatusAddedOrRemoved(TreeCacheEvent.Type type, String path) {
        return (type == TreeCacheEvent.Type.NODE_ADDED || type == TreeCacheEvent.Type.NODE_REMOVED) && path
                .matches(ShrineExecutorsNode.getJobServersExecutorStatusNodePathRegex(jobName));
    }
}
