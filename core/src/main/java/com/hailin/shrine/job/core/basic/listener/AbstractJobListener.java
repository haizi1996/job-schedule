package com.hailin.shrine.job.core.basic.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

/**
 * 作业注册中心的监听器
 * @author zhanghailin
 */
public abstract class AbstractJobListener implements TreeCacheListener {

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent treeCacheEvent) throws Exception {
        String path = null == treeCacheEvent.getData() ? "" : treeCacheEvent.getData().getPath();
        if (path.isEmpty()) {
            return;
        }
        dataChanged(client, treeCacheEvent, path);
    }
    protected abstract void dataChanged(final CuratorFramework client, final TreeCacheEvent event, final String path);
}
