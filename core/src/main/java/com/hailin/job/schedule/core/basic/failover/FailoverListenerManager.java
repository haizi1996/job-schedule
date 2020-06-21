package com.hailin.job.schedule.core.basic.failover;

import com.google.common.collect.Sets;
import com.hailin.job.schedule.core.basic.execution.ExecutionNode;
import com.hailin.job.schedule.core.basic.execution.ExecutionService;
import com.hailin.job.schedule.core.basic.sharding.ShardingService;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.listener.AbstractJobListener;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.basic.config.ConfigurationNode;
import com.hailin.job.schedule.core.basic.instance.InstanceNode;
import com.hailin.job.schedule.core.basic.storage.JobNodePath;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * 失效转移监听管理器
 * @author zhanghailin
 */
public class FailoverListenerManager extends AbstractListenerManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(FailoverListenerManager.class);

    private volatile boolean isShutdown = false;

    private ConfigurationService configService;

    private final ExecutionService executionService;

    //线程池
    private ExecutorService executorService;

    private final  String executionPath  ;

    private final Set<String> runningAndFailoverPath;

    private final FailoverService failoverService;


    public FailoverListenerManager(final JobScheduler jobScheduler) {

        super(jobScheduler);
        configService = jobScheduler.getConfigService();
        executionService = jobScheduler.getExecutionService();
        failoverService = jobScheduler.getFailoverService();
        executionPath = JobNodePath.getNodeFullPath(jobName, ExecutionNode.ROOT);
        runningAndFailoverPath = new HashSet<>();
    }

    @Override
    public void start() {
        zkCacheManager.addTreeCacheListener(new ExecutionPathListener(), executionPath, 1);

    }

    @Override
    public void shutdown() {
        super.shutdown();
        isShutdown = true;
        zkCacheManager.closeTreeCache(executionPath, 1);
        closeRunningAndFailoverNodeCaches();
    }

    private void closeRunningAndFailoverNodeCaches() {
        for (String nodePath : runningAndFailoverPath) {
            zkCacheManager.closeNodeCache(nodePath);
        }
    }


    private synchronized void failover(final Integer item) {
        if (jobScheduler == null || jobScheduler.getJob() == null) {
            return;
        }
        if (!jobScheduler.getJob().isFailoverSupported() || !configService.isFailover() || executionService
                .isCompleted(item)) {
            return;
        }

        failoverService.createCrashedFailoverFlag(item);

        if (!executionService.hasRunningItems(jobScheduler.getShardingService().getLocalHostShardingItems())) {
            failoverService.failoverIfNecessary();
        }
    }

    /**
     * 失效转移的监听器
     */
    class ExecutionPathListener extends AbstractJobListener{

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            try {
                if(isShutdown){
                    return;
                }
                //没有失效的分片项
                if (executionPath.equals(path)){
                    return;
                }
                int item = getItem(path);
                String runningPath = JobNodePath.getNodeFullPath(jobName, ExecutionNode.getRunningNode(item));
                String failoverPath = JobNodePath.getNodeFullPath(jobName,
                        FailoverNode.getExecutionFailoverNode(item));
                switch (event.getType()) {
                    case NODE_ADDED:
                        zkCacheManager.addNodeCacheListener(new RunningPathListener(item), runningPath);
                        runningAndFailoverPath.add(runningPath);
                        zkCacheManager.addNodeCacheListener(new FailoverPathJobListener(item), failoverPath);
                        runningAndFailoverPath.add(failoverPath);
                        break;
                    case NODE_REMOVED:
                        zkCacheManager.closeNodeCache(runningPath);
                        runningAndFailoverPath.remove(runningPath);
                        zkCacheManager.closeNodeCache(failoverPath);
                        runningAndFailoverPath.remove(failoverPath);
                        break;
                    default:
                }
            }catch (Exception e){
                LOGGER.error( e.getMessage(), e);
            }
        }

        //获取分片项
        private int getItem(String path){
            return Integer.parseInt(path.substring(path.lastIndexOf("/")+ 1));
        }
    }
    class RunningPathListener implements NodeCacheListener {

        private int item;

        public RunningPathListener(int item) {
            this.item = item;
        }

        @Override
        public void nodeChanged() throws Exception {
            zkCacheManager.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (isShutdown) {
                            return;
                        }
                        if (!executionService.isRunning(item)) {
                            failover(item);
                        }
                    } catch (Throwable t) {
                        LOGGER.error( jobName, t.getMessage(), t);
                    }
                }
            });
        }
    }



    class FailoverPathJobListener implements NodeCacheListener {

        private int item;

        public FailoverPathJobListener(int item) {
            this.item = item;
        }
        @Override
        public void nodeChanged() throws Exception {
            zkCacheManager.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (isShutdown) {
                            return;
                        }
                        if (!executionService.isFailover(item)) {
                            failover(item);
                        }
                    } catch (Throwable t) {
                        LOGGER.error(t.getMessage(), t);
                    }
                }
            });
        }
    }
}
