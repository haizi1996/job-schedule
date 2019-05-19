package com.hailin.shrine.job.core.basic.analyse;

import com.hailin.shrine.job.core.basic.listener.AbstractJobListener;
import com.hailin.shrine.job.core.basic.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.basic.statistics.ProcessCountStatistics;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyseResetListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyseResetListenerManager.class);

    private boolean isShutdown = false;

    public AnalyseResetListenerManager(JobScheduler jobScheduler) {
        super(jobScheduler);
    }
    @Override
    public void start() {
        zkCacheManager.addTreeCacheListener(new AnalyseResetPathListener(),
                JobNodePath.getNodeFullPath(jobName, AnalyseNode.RESET), 0);
    }

    class AnalyseResetPathListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (isShutdown) {
                return;
            }
            if (JobNodePath.getNodeFullPath(jobName, AnalyseNode.RESET).equals(path)
                    && (TreeCacheEvent.Type.NODE_UPDATED == event.getType() || TreeCacheEvent.Type.NODE_ADDED == event.getType())) {
                if (ResetCountType.RESET_ANALYSE.equals(new String(event.getData().getData()))) {
                    LOGGER.info( jobName, "job:{} reset anaylse count.", jobName);
                    ProcessCountStatistics.resetAnalyseCount(executorName, jobName);
                } else if (ResetCountType.RESET_SERVERS.equals(new String(event.getData().getData()))) {
                    LOGGER.info( jobName, "job:{} reset success/failure count", jobName);
                    ProcessCountStatistics.resetSuccessFailureCount(executorName, jobName);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        isShutdown = true;
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName , AnalyseNode.RESET) , 0);
    }
}
