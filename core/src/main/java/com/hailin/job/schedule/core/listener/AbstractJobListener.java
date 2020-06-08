package com.hailin.job.schedule.core.listener;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

/**
 * 作业注册中心的监听器
 * @author zhanghailin
 */
public abstract class AbstractJobListener implements TreeCacheListener {

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        if (null == childData) {
            return;
        }
        String path = null == event.getData() ? "" : event.getData().getPath();
        if (path.isEmpty()) {
            return;
        }
        dataChanged(client, event, null == childData.getData() ? "" : new String(childData.getData(), Charsets.UTF_8), path);
    }
    protected abstract void dataChanged(final CuratorFramework client, final TreeCacheEvent event, final String data ,final String path);
}
