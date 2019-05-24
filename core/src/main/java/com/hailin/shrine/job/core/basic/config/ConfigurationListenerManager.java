package com.hailin.shrine.job.core.basic.config;

import com.hailin.shrine.job.core.basic.execution.ExecutionContextService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.failover.FailoverService;
import com.hailin.shrine.job.core.basic.listener.AbstractJobListener;
import com.hailin.shrine.job.core.basic.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.service.ConfigurationService;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ConfigurationListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationListenerManager.class);

    private boolean isShutdown = false;

    private ExecutionContextService executionContextService;

    private ExecutionService executionService;

    private FailoverService failoverService;

    private ConfigurationService configurationService;

    public ConfigurationListenerManager(JobScheduler jobScheduler) {
        super(jobScheduler);
        jobConfiguration = jobScheduler.getCurrentConf();
        jobName = jobScheduler.getJobName();
        executionContextService = jobScheduler.getExecutionContextService();
        executionService = jobScheduler.getExecutionService();
        failoverService = jobScheduler.getFailoverService();
        configurationService = jobScheduler.getConfigService();
    }

    @Override
    public void start() {
        zkCacheManager.addTreeCacheListener(new CronPathListener(),
                JobNodePath.getNodeFullPath(jobName, ConfigurationNode.CRON), 0);
        zkCacheManager.addTreeCacheListener(new DownStreamPathListener(),
                JobNodePath.getNodeFullPath(jobName, ConfigurationNode.DOWN_STREAM), 0);
        zkCacheManager.addTreeCacheListener(new EnabledPathListener(),
                JobNodePath.getNodeFullPath(jobName, ConfigurationNode.ENABLED), 0);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        isShutdown = true;
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.CRON), 0);
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.DOWN_STREAM), 0);
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.ENABLED), 0);
    }

    class CronPathListener extends  AbstractJobListener{

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (isShutdown) {
                return;
            }
            if (ConfigurationNode.isDownStreamPath(jobName, path)
                    && TreeCacheEvent.Type.NODE_UPDATED == event.getType()) {
                LOGGER.info(jobName, "{} 's cron update", jobName);

                String cronFromZk = jobConfiguration.getCronFromZk(); // will update local cron cache
                if (!jobScheduler.getPreviousConfig().getCron().equals(cronFromZk)) {
                    jobScheduler.getPreviousConfig().setCron(cronFromZk);
                    jobScheduler.reInitializeTrigger();
                    executionService.updateNextFireTime(executionContextService.getShardingItems());
                }
            }
        }
    }

    class EnabledPathListener extends AbstractJobListener{
        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (isShutdown){
                return;
            }
            if (ConfigurationNode.isEnabledPath(jobName , path)
            && TreeCacheEvent.Type.NODE_UPDATED == event.getType()){
                Boolean isJobEnabled = Boolean.valueOf(new String(event.getData().getData()));
                LOGGER.info(jobName , "{} is enabled change to {}" , jobName , isJobEnabled);
                jobConfiguration.reLoadConfig();
                if (isJobEnabled){
                    if (jobScheduler != null && jobScheduler.getJob() != null){
                        return;
                    }
                    if (jobScheduler.getReportService() != null){
                        jobScheduler.getReportService().clearInfoMap();
                    }
                    failoverService.removeFailoverInfo();
                    jobScheduler.getJob().enableJob();;
                    configurationService.notifyJobEnabledIfNecessary();
                }else {
                    if (jobScheduler != null && jobScheduler.getJob() != null){
                        return;
                    }
                    jobScheduler.getJob().disableJob();
                    failoverService.removeFailoverInfo();
                    configurationService.notifyJobDisabled();
                }
            }
        }
    }


    class DownStreamPathListener extends AbstractJobListener{
        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (isShutdown){
                return;
            }
            if (ConfigurationNode.isDownStreamPath(jobName , path)
            && TreeCacheEvent.Type.NODE_UPDATED == event.getType()){
                try {
                    byte[] data = event.getData().getData();
                    String dataStr = data == null ? "" : new String(data , StandardCharsets.UTF_8);
                    jobConfiguration.setDownStream(dataStr);
                    LOGGER.info( jobName, "{} 's downStream updated to {}", jobName, dataStr);
                } catch (Exception e) {
                    LOGGER.error( jobName, "unexpected error", e);
                }
            }
        }
    }
}
