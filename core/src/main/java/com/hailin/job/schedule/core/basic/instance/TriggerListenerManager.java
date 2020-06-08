package com.hailin.job.schedule.core.basic.instance;

import com.hailin.job.schedule.core.listener.AbstractJobListener;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

/**
 * 作业触发监听管理器
 * @author zhanghailin
 */
public class TriggerListenerManager extends AbstractListenerManager {

    private final InstanceService instanceService;

    private final InstanceNode instanceNode;

    public TriggerListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        instanceService = new InstanceService(jobName ,regCenter);
        instanceNode = new InstanceNode(jobName);
    }

    @Override
    public void start() {

    }

    class IobTriggerStatusJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (!InstanceOperation.TRIGGER.name().equals(data) || !instanceNode.isInstancePath(path) || TreeCacheEvent.Type.NODE_UPDATED != event.getType()) {
                return;
            }
            instanceService.clearTriggerFlag();
            if (!JobRegistry.getInstance().isShutdown(jobName) && !JobRegistry.getInstance().isJobRunning(jobName)) {
                // TODO 目前是作业运行时不能触发, 未来改为堆积式触发
                JobRegistry.getInstance().getJobScheduleController(jobName).triggerJob();
            }
        }
    }
}
