package com.hailin.shrine.job.core.basic.failover;

import com.google.common.collect.Sets;
import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.config.ConfigurationNode;
import com.hailin.shrine.job.core.basic.execution.ExecutionNode;
import com.hailin.shrine.job.core.basic.instance.InstanceNode;
import com.hailin.shrine.job.core.basic.listener.AbstractJobListener;
import com.hailin.shrine.job.core.basic.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.basic.server.ServerStatus;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.service.ConfigurationService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

    private final ConfigurationService configService;

    private final ShardingService shardingService;

    private final FailoverService failoverService;

    private final ConfigurationNode configNode;

    private final InstanceNode instanceNode;

    public FailoverListenerManager(final String jobName , final CoordinatorRegistryCenter regCenter) {

        super(jobName , regCenter);
        executionPath = JobNodePath.getNodeFullPath(jobName , ExecutionNode.ROOT);
        runningAndFailoverPath = Sets.newHashSet();
        configService = new ConfigurationService( jobName , regCenter);
        shardingService = new ShardingService( jobName , regCenter);
        failoverService = new FailoverService( jobName , regCenter);
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName );
    }

    @Override
    public void start() {
        addDataListener(new JobCrashedJobListener());
        addDataListener(new FailoverSettingsChangedJobListener());
    }

    private boolean isFailoverEnabled() {
        JobConfiguration jobConfig = configService.load(true);
        return null != jobConfig && jobConfig.isFailover();
    }

    /**
     * 失效转移的监听器
     */
    class ExecutionPathListener extends AbstractJobListener{

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
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


//    class RunningPathListener implements NodeCacheListener {
//
//        private int item;
//
//        public RunningPathListener(int item) {
//            this.item = item;
//        }
//
//        @Override
//        public void nodeChanged() throws Exception {
//            zkCacheManager.getExecutorService().execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        if (isShutDown) {
//                            return;
//                        }
//                        if (!executionService.isRunning(item)) {
//                            failover(item);
//                        }
//                    } catch (Throwable t) {
//                        LOGGER.error( jobName, t.getMessage(), t);
//                    }
//                }
//            });
//        }
//    }


    class JobCrashedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (isFailoverEnabled() && TreeCacheEvent.Type.NODE_REMOVED == event.getType() && instanceNode.isInstancePath(path)) {
                String jobInstanceId = path.substring(instanceNode.getInstanceFullPath().length() + 1);
                if (jobInstanceId.equals(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId())) {
                    return;
                }
                List<Integer> failoverItems = failoverService.getFailoverItems(jobInstanceId);
                if (!failoverItems.isEmpty()) {
                    for (int each : failoverItems) {
                        failoverService.setCrashedFailoverFlag(each);
                        failoverService.failoverIfNecessary();
                    }
                } else {
                    for (int each : shardingService.getShardingItems(jobInstanceId)) {
                        failoverService.setCrashedFailoverFlag(each);
                        failoverService.failoverIfNecessary();
                    }
                }
            }
        }
    }

    class FailoverSettingsChangedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (configNode.isConfigPath(path) && TreeCacheEvent.Type.NODE_UPDATED == eventType && !LiteJobConfigurationGsonFactory.fromJson(data).isFailover()) {
                failoverService.removeFailoverInfo();
            }
        }


    }
}
