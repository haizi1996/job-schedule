package com.hailin.job.schedule.core.basic.server;

import com.hailin.job.schedule.core.basic.storage.JobNodePath;
import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.job.schedule.core.listener.AbstractJobListener;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.basic.config.ConfigurationNode;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 作业控制监听管理器
 */
public class JobOperationListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobOperationListenerManager.class);

    private boolean isShutdown = false;

    private ExecutorService jobDeleteExecutorService;

    public JobOperationListenerManager(final JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        jobDeleteExecutorService = Executors.newSingleThreadExecutor(
                new ScheduleThreadFactory(executorName + "-" + jobName + "-jobDelete" , false)
        );
        zkCacheManager.addTreeCacheListener(new TriggerJobRunAtOnceListener(),
                JobNodePath.getNodeFullPath(jobName, String.format(ServerNode.RUNONETIME, executorName)), 0);
        zkCacheManager.addTreeCacheListener(new JobForcedToStopListener(),
                JobNodePath.getNodeFullPath(jobName, String.format(ServerNode.STOPONETIME, executorName)), 0);
        zkCacheManager.addTreeCacheListener(new JobDeleteListener(),
                JobNodePath.getNodeFullPath(jobName, ConfigurationNode.TO_DELETE), 0);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        isShutdown = true;
        if (jobDeleteExecutorService != null){
            jobDeleteExecutorService.shutdown();;
        }
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName , String.format(ServerNode.RUNONETIME , executorName)) , 0);
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName , String.format(ServerNode.STOPONETIME , executorName)) , 0);
        zkCacheManager.closeTreeCache(JobNodePath.getNodeFullPath(jobName , ConfigurationNode.TO_DELETE) , 0);

    }

    class TriggerJobRunAtOnceListener extends AbstractJobListener {
        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data ,  String path) {
            if (isShutdown){
                return;
            }
            if ((TreeCacheEvent.Type.NODE_ADDED == event.getType() || TreeCacheEvent.Type.NODE_UPDATED == event.getType())
                &&ServerNode.isRunOneTimePath(jobName , path ,executorName)){
                if (!jobScheduler.getJob().isRunning()) {
                    String triggeredDataStr = getTriggeredDataStr(event);
                    LOGGER.info( jobName, "job run-at-once triggered, triggeredData:{}", triggeredDataStr);
                    jobScheduler.triggerJob(triggeredDataStr);
                } else {
                    LOGGER.info( jobName, "job is running, run-at-once ignored.");
                }
                coordinatorRegistryCenter.remove(path);
            }
        }

        private String getTriggeredDataStr(TreeCacheEvent event) {
            String transDataStr = null;
            try {
                byte[] data = event.getData().getData();
                if (data != null) {
                    transDataStr = new String(data, StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                LOGGER.error( jobName, "unexpected error", e);
            }
            return transDataStr;
        }
    }

    class  JobDeleteListener extends AbstractJobListener{
        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (isShutdown){
                return;
            }
            if (ConfigurationNode.isToDeletePath(jobName , path)
            && (TreeCacheEvent.Type.NODE_ADDED == event.getType()
                    || TreeCacheEvent.Type.NODE_UPDATED == event.getType())){
                LOGGER.info(jobName , "job is going to be deleted.");
                jobDeleteExecutorService.execute(()->{
                    try {
                        jobScheduler.shutdown(true);
                    }catch (Throwable t){
                        LOGGER.error(jobName , "delete job error" , t );
                    }
                });
            }
        }
    }

    /**
     * 作业终止的监听器
     */
    class JobForcedToStopListener extends AbstractJobListener{

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event,String data ,  String path) {
            if (isShutdown){
                return;
            }
            if (TreeCacheEvent.Type.NODE_ADDED == event.getType() ||
                    TreeCacheEvent.Type.NODE_UPDATED == event.getType()){
                try {
                    LOGGER.info( jobName, "job is going to be stopped at once.");
                    jobScheduler.getJob().forceStop();
                }finally {
                    coordinatorRegistryCenter.remove(path);
                }
            }
        }
    }
}
