package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

@AllArgsConstructor
public class JobConfigTriggerShardingListener extends AbstractTreeCacheListener {

    private String jobName;
    private NamespaceShardingService namespaceShardingService;
    private String enabledPath;
    private String forceShardPath;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        if (isJobEnabledPath(type, path)) {
            if (Boolean.parseBoolean(nodeData)) {
                namespaceShardingService.asyncShardingWhenJobEnable(jobName);
            } else {
                namespaceShardingService.asyncShardingWhenJobDisable(jobName);
            }
        } else if (isForceShardJob(type, path)) {
            namespaceShardingService.asyncShardingWhenJobForceShard(jobName);
        }
    }

    private boolean isJobEnabledPath(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_UPDATED && path.equals(enabledPath);
    }

    private boolean isForceShardJob(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_ADDED && path.equals(forceShardPath);
    }
}
