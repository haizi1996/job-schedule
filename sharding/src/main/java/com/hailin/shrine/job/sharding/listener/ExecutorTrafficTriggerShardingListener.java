package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class ExecutorTrafficTriggerShardingListener extends AbstractTreeCacheListener{

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorTrafficTriggerShardingListener.class);

    private NamespaceShardingService namespaceShardingService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        try {
            if (isExecutorNoTraffic(type, path)) {
                String executorName = ShrineExecutorsNode.getExecutorNameByNoTrafficPath(path);
                namespaceShardingService.asyncShardingWhenExtractExecutorTraffic(executorName);
            } else if (isExecutorTraffic(type, path)) {
                String executorName = ShrineExecutorsNode.getExecutorNameByNoTrafficPath(path);
                namespaceShardingService.asyncShardingWhenRecoverExecutorTraffic(executorName);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
    private boolean isExecutorNoTraffic(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_ADDED && path.matches(ShrineExecutorsNode.EXECUTOR_NO_TRAFFIC_NODE_PATH_REGEX);
    }

    public boolean isExecutorTraffic(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_REMOVED && path.matches(ShrineExecutorsNode.EXECUTOR_NO_TRAFFIC_NODE_PATH_REGEX);
    }
}
