package com.hailin.job.schedule.core.basic.election;

import com.hailin.job.schedule.core.basic.storage.JobNodePath;
import com.hailin.job.schedule.core.listener.AbstractListenerManager;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 主节点选举监听器
 * @author zhanghailin
 */
public class ElectionListenerManager extends AbstractListenerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionListenerManager.class);

    private boolean isShutdown;

    private final LeaderElectionService leaderElectionService;


    public ElectionListenerManager(final JobScheduler jobScheduler) {
        super(jobScheduler);
        this.leaderElectionService = new LeaderElectionService(jobScheduler);
    }

    @Override
    public void start() {
        zkCacheManager.addNodeCacheListener(new LeaderElectionJobListener(),
                JobNodePath.getNodeFullPath(jobName, ElectionNode.LEADER_HOST));
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        leaderElectionService.shutdown();
        zkCacheManager.closeNodeCache(JobNodePath.getNodeFullPath(jobName , LeaderNode.INSTANCE));
    }

    /**
     * leader选举的监听器
     * 所有follow进行监听 leader失效后，进行leader选举
     */
    class LeaderElectionJobListener implements NodeCacheListener {
        @Override
        public void nodeChanged() throws Exception {
            zkCacheManager.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        LOGGER.debug("Leader host nodeChanged", jobName);
                        if (isShutdown) {
                            LOGGER.debug("ElectionListenerManager has been shutdown");
                            return;
                        }
                        if (!leaderElectionService.hasLeader()) {
                            LOGGER.info( "Leader crashed, elect a new leader now");
                            leaderElectionService.leaderElection();
                            LOGGER.info("Leader election completed");
                        } else {
                            LOGGER.debug("Leader is already existing, unnecessary to election");
                        }
                    } catch (Throwable t) {
                        LOGGER.error( t.getMessage(), t);
                    }
                }
            });
        }
    }

}
