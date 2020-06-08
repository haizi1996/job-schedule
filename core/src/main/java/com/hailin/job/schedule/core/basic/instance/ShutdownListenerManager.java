package com.hailin.job.schedule.core.basic.instance;

import com.hailin.job.schedule.core.listener.AbstractJobListener;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.basic.schdule.SchedulerFacade;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

/**
 * 运行实例关闭监听器管理器
 * @author zhanghailin
 */
public class ShutdownListenerManager extends AbstractListenerManager {

    private final InstanceNode instanceNode;

    private final InstanceService instanceService;

    private final SchedulerFacade schedulerFacade;

    public ShutdownListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        instanceNode = new InstanceNode(jobName);
        instanceService = new InstanceService(jobName , regCenter);
        schedulerFacade = new SchedulerFacade(jobName , regCenter);
    }

    @Override
    public void start() {
        addDataListener(new InstanceShutdownStatusJobListener());
    }
    class InstanceShutdownStatusJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && !JobRegistry.getInstance().getJobScheduleController(jobName).isPaused()
                    && isRemoveInstance(path, event.getType()) && !isReconnectedRegistryCenter()) {
                schedulerFacade.shutdownInstance();
            }
        }

        private boolean isRemoveInstance(final String path, final TreeCacheEvent.Type eventType) {
            return instanceNode.isInstancePath(path) && TreeCacheEvent.Type.NODE_REMOVED == eventType;
        }

        private boolean isReconnectedRegistryCenter() {
            return instanceService.isLocalJobInstanceExisted();
        }
    }
}
