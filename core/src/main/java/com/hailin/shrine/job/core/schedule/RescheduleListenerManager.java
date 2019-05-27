package com.hailin.shrine.job.core.schedule;

import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.config.ConfigurationNode;
import com.hailin.shrine.job.core.config.JobConfiguration;
import com.hailin.shrine.job.core.listener.AbstractJobListener;
import com.hailin.shrine.job.core.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

/**
 * 重新调度员监听器
 * @author zhanghailin
 */
public class RescheduleListenerManager extends AbstractListenerManager {

    private final ConfigurationNode configNode;


    public RescheduleListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        configNode = new ConfigurationNode(jobName);
    }

    @Override
    public void start() {
        addDataListener(new CronSettingAndJobEventChangedJobListener());
    }


    class CronSettingAndJobEventChangedJobListener extends AbstractJobListener{
        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (configNode.isConfigPath(path) && TreeCacheEvent.Type.NODE_UPDATED == event.getType() && !JobRegistry.getInstance().isShutdown(jobName)) {
                JobRegistry.getInstance().getJobScheduleController(jobName).rescheduleJob(JsonUtils.fromJson(data , JobConfiguration.class).getTypeConfig().getCoreConfig().getCron());
            }
        }
    }
}
