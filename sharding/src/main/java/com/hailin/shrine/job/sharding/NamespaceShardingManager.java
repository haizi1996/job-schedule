package com.hailin.shrine.job.sharding;

import com.hailin.shrine.job.integrate.service.ReportAlarmService;
import com.hailin.shrine.job.integrate.service.UpdateJobConfigService;
import com.hailin.shrine.job.sharding.listener.AbstractConnectionListener;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import com.hailin.shrine.job.sharding.service.AddJobListenersService;
import com.hailin.shrine.job.sharding.service.ExecutorCleanService;
import com.hailin.shrine.job.sharding.service.NamespaceShardingService;
import com.hailin.shrine.job.sharding.service.ShardingTreeCacheService;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class NamespaceShardingManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceShardingService.class);

    private NamespaceShardingService namespaceShardingService;

    private ExecutorCleanService executorCleanService;

    private CuratorFramework curatorFramework;

    private AddJobListenersService addJobListenersService;

    private ShardingTreeCacheService shardingTreeCacheService;

    private NamespaceShardingConnectionListener namespaceShardingConnectionListener;

    private String namespace;

    private String zkClusterKey;

    public NamespaceShardingManager(CuratorFramework curatorFramework, String namespace, String hostValue,
                                    ReportAlarmService reportAlarmService, UpdateJobConfigService updateJobConfigService) {
        this.curatorFramework = curatorFramework;
        this.namespace = namespace;
        this.shardingTreeCacheService = new ShardingTreeCacheService(namespace, curatorFramework);
        this.namespaceShardingService = new NamespaceShardingService(curatorFramework, hostValue,
                reportAlarmService);
        this.executorCleanService = new ExecutorCleanService(curatorFramework,updateJobConfigService);
        this.addJobListenersService = new AddJobListenersService(namespace, curatorFramework, namespaceShardingService,
                shardingTreeCacheService);
    }
    private void start0() throws Exception {
        shardingTreeCacheService.start();
        // create ephemeral node $SaturnExecutors/leader/host & $Jobs.
        namespaceShardingService.leaderElection();
        addJobListenersService.addExistJobPathListener();
        addOnlineOfflineListener();
        addExecutorShardingListener();
        addLeaderElectionListener();
        addNewOrRemoveJobListener();
    }

    private void addNewOrRemoveJobListener() throws Exception {
        String path = ShrineExecutorsNode.JOBSNODE_PATH;
        int depth = 1;
        createNodePathIfNotExists(path);
        shardingTreeCacheService.addTreeCacheIfAbsent(path, depth);
        shardingTreeCacheService.addTreeCacheListenerIfAbsent(path, depth,
                new AddOrRemoveJobListener(addJobListenersService));
    }

    private void addLeaderElectionListener() throws Exception {
        String path = ShrineExecutorsNode.LEADERNODE_PATH;
        int depth = 1;
        createNodePathIfNotExists(path);
        shardingTreeCacheService.addTreeCacheIfAbsent(path, depth);
        shardingTreeCacheService.addTreeCacheListenerIfAbsent(path, depth,
                new LeadershipElectionListener(namespaceShardingService));
    }

    private void addExecutorShardingListener() throws Exception {
        String path = ShrineExecutorsNode.SHARDINGNODE_PATH;
        int depth = 1;
        createNodePathIfNotExists(path);
        shardingTreeCacheService.addTreeCacheIfAbsent(path, depth);
        shardingTreeCacheService.addTreeCacheListenerIfAbsent(path, depth,
                new SaturnExecutorsShardingTriggerShardingListener(namespaceShardingService));
    }

    /**
     * watch 2-level-depth of the children of /$SaturnExecutors/executors
     */
    private void addOnlineOfflineListener() throws Exception {
        String path = ShrineExecutorsNode.EXECUTORSNODE_PATH;
        int depth = 2;
        createNodePathIfNotExists(path);
        shardingTreeCacheService.addTreeCacheIfAbsent(path, depth);
        shardingTreeCacheService.addTreeCacheListenerIfAbsent(path, depth,
                new ExecutorOnlineOfflineTriggerShardingListener(namespaceShardingService, executorCleanService));
        shardingTreeCacheService.addTreeCacheListenerIfAbsent(path, depth,
                new ExecutorTrafficTriggerShardingListener(namespaceShardingService));
    }

    private void createNodePathIfNotExists(String path) throws Exception {
        if (curatorFramework.checkExists().forPath(path) == null) {
            try {
                curatorFramework.create().creatingParentsIfNeeded().forPath(path);
            } catch (KeeperException.NodeExistsException e) {// NOSONAR
                LOGGER.info("node {} already existed, so skip creation", path);
            }
        }
    }

    /**
     * close listeners, delete leadership
     */
    public void stop() {
        try {
            if (namespaceShardingConnectionListener != null) {
                curatorFramework.getConnectionStateListenable().removeListener(namespaceShardingConnectionListener);
                namespaceShardingConnectionListener.shutdownNowUntilTerminated();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            shardingTreeCacheService.shutdown();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            namespaceShardingService.shutdown();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
    public void start() throws Exception {
        start0();
        addConnectionLostListener();
    }

    private void addConnectionLostListener() {
        namespaceShardingConnectionListener = new NamespaceShardingConnectionListener("connectionListener-for-NamespaceSharding-" + namespace);
        curatorFramework.getConnectionStateListenable().addListener(namespaceShardingConnectionListener);
    }

    public void stopWithCurator(){
        stop();
        curatorFramework.close();
    }

    class NamespaceShardingConnectionListener extends AbstractConnectionListener{

        public NamespaceShardingConnectionListener(String threadName) {
            super(threadName);
        }

        @Override
        public void stop() {
            try {
                shardingTreeCacheService.shutdown();
                namespaceShardingService.shutdown();
            } catch (InterruptedException e) {
                LOGGER.info("stop interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.error("stop error", e);
            }
        }

        @Override
        public void restart() {
            try {
                start0();
            } catch (InterruptedException e) {
                LOGGER.info("restart interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.error("restart error", e);
            }
        }



    }
}
