package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.ExecutorCleanService;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class ExecutorOnlineOfflineTriggerShardingListener extends AbstractTreeCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorOnlineOfflineTriggerShardingListener.class);

    private NamespaceShardingService namespaceShardingService;
    private ExecutorCleanService executorCleanService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        if (isExecutorOnline(type, path)) {
            String executorName = ShrineExecutorsNode.getExecutorNameByIpPath(path);
            namespaceShardingService.asyncShardingWhenExecutorOnline(executorName, nodeData);
        } else if (isExecutorOffline(type, path)) {
            String executorName = ShrineExecutorsNode.getExecutorNameByIpPath(path);
            try {
                executorCleanService.clean(executorName);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
            try {
                namespaceShardingService.asyncShardingWhenExecutorOffline(executorName);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
    private boolean isExecutorOnline(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_ADDED && path.matches(ShrineExecutorsNode.EXECUTOR_IPNODE_PATH_REGEX);
    }

    public boolean isExecutorOffline(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_REMOVED && path.matches(ShrineExecutorsNode.EXECUTOR_IPNODE_PATH_REGEX);
    }
}
