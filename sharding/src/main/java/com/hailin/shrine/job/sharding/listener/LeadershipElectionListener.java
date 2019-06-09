package com.hailin.shrine.job.sharding.listener;

import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import lombok.AllArgsConstructor;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

@AllArgsConstructor
public class LeadershipElectionListener extends AbstractTreeCacheListener {

    private NamespaceShardingService namespaceShardingService;

    @Override
    protected void childEvent(TreeCacheEvent.Type type, String path, String nodeData) throws Exception {
        if (isLeaderRemove(type, path)) {
            namespaceShardingService.leaderElection();
        }
    }

    private boolean isLeaderRemove(TreeCacheEvent.Type type, String path) {
        return type == TreeCacheEvent.Type.NODE_REMOVED && ShrineExecutorsNode.LEADER_HOSTNODE_PATH.equals(path);
    }
}
