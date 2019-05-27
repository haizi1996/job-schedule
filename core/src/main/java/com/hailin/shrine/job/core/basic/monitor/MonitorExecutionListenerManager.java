package com.hailin.shrine.job.core.basic.monitor;

import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.core.basic.config.ConfigurationNode;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.config.JobConfiguration;
import com.hailin.shrine.job.core.listener.AbstractJobListener;
import com.hailin.shrine.job.core.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

/**
 * 幂等性监听管理器
 * @author zhanghailin
 */
public class MonitorExecutionListenerManager extends AbstractListenerManager {

    private final ConfigurationNode configNode;

    private ExecutionService executionService;

    public MonitorExecutionListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        configNode = new ConfigurationNode(jobName);
        executionService = new ExecutionService(jobName ,regCenter);
    }

    @Override
    public void start() {
        addDataListener(new MonitorExecutionSettingsChangedJobListener());
    }

        class MonitorExecutionSettingsChangedJobListener extends AbstractJobListener {

            @Override
            protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
                if (configNode.isConfigPath(path) && TreeCacheEvent.Type.NODE_UPDATED == event.getType() && !JsonUtils.fromJson(data , JobConfiguration.class).isMonitorExecution()) {
                    executionService.clearAllRunningInfo();
                }
            }
        }
}
