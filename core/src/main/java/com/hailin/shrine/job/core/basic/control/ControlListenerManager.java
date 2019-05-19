package com.hailin.shrine.job.core.basic.control;

import com.hailin.shrine.job.core.basic.listener.AbstractJobListener;
import com.hailin.shrine.job.core.basic.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControlListenerManager.class);

    private boolean isShutdown = false;

    private ReportService reportService;

    public ControlListenerManager(JobScheduler jobScheduler) {
        super(jobScheduler);
        reportService = jobScheduler.getReportService();
    }

    @Override
    public void start() {
        zkCacheManager.addTreeCacheListener(new ReportPathListener(),
                JobNodePath.getNodeFullPath(jobName, ControlNode.REPORT_NODE), 0);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        isShutdown = true;
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName, ControlNode.REPORT_NODE), 0);
    }

    class ReportPathListener extends AbstractJobListener{

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (isShutdown){
                return;
            }
            if (ControlNode.isReportPath(jobName , path)
                && TreeCacheEvent.Type.NODE_ADDED == event.getType() ){
                LOGGER.info( jobName, "{} received report event from console, start to flush data to zk.",
                        jobName);
                reportService.reportDataToZK();
            }
        }
    }

}
