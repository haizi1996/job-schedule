package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import com.hailin.shrine.job.sharding.service.ShardingTreeCacheService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_ADDED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_REMOVED;

/**
 * Job Server上下线Listener，会检测/status节点的添加和删除行为。
 */
@AllArgsConstructor
public class JobServersOnlineOfflineListener  extends AbstractTreeCacheListener{

    private static final String NODE_STATUS = "/status";

    private static final int TREE_CACHE_DEPTH = 0;

    private String jobName;

    private String jobServersNodePath;

    private ShardingTreeCacheService shardingTreeCacheService;

    private NamespaceShardingService namespaceShardingService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        if (path.equals(jobServersNodePath)) {
            return;
        }

        String statusPath = path + NODE_STATUS;
        if (type == NODE_ADDED) {
            shardingTreeCacheService.addTreeCacheIfAbsent(statusPath, TREE_CACHE_DEPTH);
            shardingTreeCacheService.addTreeCacheListenerIfAbsent(statusPath, TREE_CACHE_DEPTH,
                    new JobServersTriggerShardingListener(jobName, namespaceShardingService));
        } else if (type == NODE_REMOVED) { // 保证只watch新server clean事件
            shardingTreeCacheService.removeTreeCache(statusPath, TREE_CACHE_DEPTH);
        }
    }
}
