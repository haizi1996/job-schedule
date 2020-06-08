package com.hailin.job.schedule.core.basic.sharding;

import com.hailin.job.schedule.core.listener.AbstractJobListener;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.basic.config.ConfigurationNode;
import com.hailin.job.schedule.core.basic.instance.InstanceNode;
import com.hailin.job.schedule.core.basic.server.ServerNode;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * 分片监听管理器
 */
public class ShardingListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingListenerManager.class);

    private volatile boolean isShutdown ;

    private CuratorWatcher necessaryWatcher;

    private ShardingService shardingService;

    private ExecutorService executorService;

    private ConnectionStateListener connectionStateListener;
    private final ConfigurationNode configNode;
    private final InstanceNode instanceNode ;
    private final ServerNode serverNode ;


    public ShardingListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
        serverNode = new ServerNode(jobName);
        shardingService = new ShardingService( jobName , regCenter);
    }

    @Override
    public void start() {
//        if (necessaryWatcher != null){
//            executorService = Executors.newSingleThreadExecutor(
//                    new ShrineThreadFactory(executorName + "-" + jobName + "-registerNecessaryWatcher", false));
//
//            shardingService.registerNecessaryWatcher(necessaryWatcher);
//            connectionStateListener = (client , newState)->{
//                if (newState == ConnectionState.CONNECTED ||
//                newState == ConnectionState.RECONNECTED){
//                    LOGGER.info( jobName,
//                            "state change to {}, trigger doBusiness and register necessary watcher.", newState);
//                    doBusiness();
//                    registerNecessaryWatcher();
//                }
//            };
//            addConnectionStateListener(connectionStateListener);
//        }
    }
    @Override
    public void shutdown() {
        super.shutdown();
        if (executorService != null) {
            executorService.shutdownNow();
        }
        if (connectionStateListener != null) {
            removeConnectionStateListener(connectionStateListener);
        }
        isShutdown = true;
    }

    private void registerNecessaryWatcher() {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                if (!isShutdown) {
                    shardingService.registerNecessaryWatcher();
                }
            }
        });
    }

    private void doBusiness() {
        try {
            // cannot block reconnected thread or re-registerNecessaryWatcher, so use thread pool to do business,
            // and the thread pool is the same with job-tree-cache's
            zkCacheManager.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (isShutdown) {
                            return;
                        }
                        if (jobScheduler == null || jobScheduler.getJob() == null) {
                            return;
                        }
                        LOGGER.info( jobName, "{} trigger on-resharding", jobName);
                        jobScheduler.getJob().onResharding();
                    } catch (Throwable t) {
                        LOGGER.error( jobName, "Exception throws during resharding", t);
                    }
                }
            });
        } catch (Throwable t) {
            LOGGER.error( jobName, "Exception throws during execute thread", t);
        }
    }

    class NecessaryWatcher implements CuratorWatcher{

        @Override
        public void process(WatchedEvent event) throws Exception {
            if (isShutdown){
                return;
            }
            switch (event.getType())
            {
                case NodeCreated:

                case NodeDataChanged:
                    LOGGER.info( jobName, "event type:{}, path:{}", event.getType(), event.getPath());
                    doBusiness();
                    default:
                        registerNecessaryWatcher();
            }

        }
    }


    class ListenServersChangedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isInstanceChange(event.getType(), path) || isServerChange(path))) {
                shardingService.setReshardingFlag();
            }
        }

        private boolean isInstanceChange(final TreeCacheEvent.Type eventType, final String path) {
            return instanceNode.isInstancePath(path) && TreeCacheEvent.Type.NODE_UPDATED != eventType;
        }

        private boolean isServerChange(final String path) {
            return serverNode.isLocalServerPath(path);
        }
    }
}
