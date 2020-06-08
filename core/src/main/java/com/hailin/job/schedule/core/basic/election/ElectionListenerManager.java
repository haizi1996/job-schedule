package com.hailin.job.schedule.core.basic.election;

import com.hailin.job.schedule.core.listener.AbstractJobListener;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.basic.JobRegistry;
import com.hailin.job.schedule.core.basic.server.ServerNode;
import com.hailin.job.schedule.core.basic.server.ServerService;
import com.hailin.job.schedule.core.basic.server.ServerStatus;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 主节点选举监听器
 * @author zhanghailin
 */
public class ElectionListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionListenerManager.class);

    private boolean isShutDown;

    private final LeaderElectionService leaderElectionService;
    private final ServerService serverService;

    private final LeaderNode leaderNode;

    private final ServerNode serverNode;


    public ElectionListenerManager(String jobName, CoordinatorRegistryCenter regCenter) {
        super(jobName, regCenter);
        this.leaderElectionService = new LeaderElectionService(jobName , regCenter);
        serverService = new ServerService(jobName , regCenter);
        leaderNode = new LeaderNode(jobName);
        serverNode = new ServerNode(jobName);
    }

    @Override
    public void start() {
//        zkCacheManager.addNodeCacheListener(new LeaderElectionJobListener() , JobNodePath.getNodeFullPath(jobName, ElectionNode.LEADER_HOST));
        addDataListener(new LeaderElectionJobListener());
        addDataListener(new LeaderAbdicationJobListener());

    }

    @Override
    public void shutdown() {
        isShutDown = false;
        leaderElectionService.shutdown();
//        zkCacheManager.closeNodeCache(JobNodePath.getNodeFullPath(jobNameame , LeaderNode.LEADER_HOST));
    }

    /**
     * leader选举的监听器
     * 所有follow进行监听 leader失效后，进行leader选举
     */
    class LeaderElectionJobListener extends AbstractJobListener {
        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isActiveElection(path, data) || isPassiveElection(path, event.getType()))) {
                leaderElectionService.leaderElection();
            }
        }
        private boolean isActiveElection(final String path, final String data) {
            return !leaderElectionService.hasLeader() && isLocalServerEnabled(path, data);
        }

        private boolean isPassiveElection(final String path, final TreeCacheEvent.Type eventType) {
            return isLeaderCrashed(path, eventType) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp());
        }

        private boolean isLeaderCrashed(final String path, final TreeCacheEvent.Type eventType) {
            return leaderNode.isLeaderInstancePath(path) && TreeCacheEvent.Type.NODE_REMOVED == eventType;
        }

        private boolean isLocalServerEnabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && !ServerStatus.DISABLED.name().equals(data);
        }
    }

    class LeaderAbdicationJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String data, String path) {
            if (leaderElectionService.isLeader() && isLocalServerDisabled(path, data)) {
                leaderElectionService.removeLeader();
            }
        }

        private boolean isLocalServerDisabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && ServerStatus.DISABLED.name().equals(data);
        }
    }
}
