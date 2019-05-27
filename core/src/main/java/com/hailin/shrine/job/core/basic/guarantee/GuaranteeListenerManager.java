package com.hailin.shrine.job.core.basic.guarantee;

import com.hailin.shrine.job.core.listener.AbstractDistributeOnceElasticJobListener;
import com.hailin.shrine.job.core.listener.AbstractJobListener;
import com.hailin.shrine.job.core.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.listener.ElasticJobListener;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

import java.util.List;

public class GuaranteeListenerManager extends AbstractListenerManager {

    private final GuaranteeNode guaranteeNode;

    private List<ElasticJobListener> elasticJobListeners;

    public GuaranteeListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        this.guaranteeNode = new GuaranteeNode(jobName);
    }

    @Override
    public void start() {
        addDataListener(new StartedNodeRemovedJobListener());
        addDataListener(new CompletedNodeRemovedJobListener());
    }
    class StartedNodeRemovedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (TreeCacheEvent.Type.NODE_REMOVED == event.getType() && guaranteeNode.isStartedRootNode(path)) {
                for (ElasticJobListener each : elasticJobListeners) {
                    if (each instanceof AbstractDistributeOnceElasticJobListener) {
                        ((AbstractDistributeOnceElasticJobListener) each).notifyWaitingTaskStart();
                    }
                }
            }
        }
    }

    class CompletedNodeRemovedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (TreeCacheEvent.Type.NODE_REMOVED == event.getType() && guaranteeNode.isCompletedRootNode(path)) {
                for (ElasticJobListener each : elasticJobListeners) {
                    if (each instanceof AbstractDistributeOnceElasticJobListener) {
                        ((AbstractDistributeOnceElasticJobListener) each).notifyWaitingTaskComplete();
                    }
                }
            }
        }
    }
}
