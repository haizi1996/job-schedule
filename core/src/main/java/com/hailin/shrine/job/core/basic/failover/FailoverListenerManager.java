package com.hailin.shrine.job.core.basic.failover;

import com.google.common.collect.Sets;
import com.hailin.shrine.job.core.basic.execution.ExecutionNode;
import com.hailin.shrine.job.core.basic.listener.AbstractJobListener;
import com.hailin.shrine.job.core.basic.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.service.ConfigurationService;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * 失效转移监听管理器
 * @author zhanghailin
 */
public class FailoverListenerManager extends AbstractListenerManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(FailoverListenerManager.class);

    private volatile boolean isShutDown = false;

    private ConfigurationService configurationService;

    //线程池
    private ExecutorService executorService;

    private final  String executionPath  ;

    private final Set<String> runningAndFailoverPath;

    public FailoverListenerManager(JobScheduler jobScheduler) {
        super(jobScheduler);
//        configurationService = jobScheduler
        executionPath = JobNodePath.getNodeFullPath(jobName , ExecutionNode.ROOT);
        runningAndFailoverPath = Sets.newHashSet();
    }

    @Override
    public void start() {
        zkCacheManager.addTreeCacheListener();
    }

    /**
     * 失效转移的监听器
     */
    class ExecutionPathListener extends AbstractJobListener{

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            try {
                if(isShutDown){
                    return;
                }
                //没有失效的分片项
                if (executionPath.equals(path)){
                    return;
                }
                int item = getItem(path);
                String runningPath = JobNodePath.getNodeFullPath(jobName , ExecutionNode.getRunningNode(item));
                String failoverPath = JobNodePath.getNodeFullPath(jobName , FailoverNode.getExecutionFailoverNode(item));
                switch (event.getType()){
                    case NODE_ADDED:
                        zkCacheManager.addNodeCacheListener();
                }
            }catch (Throwable throwable){

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
                        if (isShutDown) {
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
}
