package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class SaturnExecutorsShardingTriggerShardingListener extends AbstractTreeCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SaturnExecutorsShardingTriggerShardingListener.class);

    private NamespaceShardingService namespaceShardingService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        if (isShardAllAtOnce(type, path)) {
            LOGGER.info("shard-all-at-once triggered.");
            namespaceShardingService.asyncShardingWhenExecutorAll();
        }
    }

    private boolean isShardAllAtOnce(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_ADDED
                && ShrineExecutorsNode.getExecutorShardingNodePath("shardAllAtOnce").equals(path);
    }
}
