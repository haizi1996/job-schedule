package com.hailin.shrine.job.core.basic.election;

import com.hailin.shrine.job.core.basic.listener.AbstractListenerManager;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
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

    public ElectionListenerManager(final JobScheduler jobScheduler){
        super(jobScheduler);
        leaderElectionService = new LeaderElectionService(jobScheduler);
    }

    @Override
    public void start() {
        zkCacheManager.addNodeCacheListener(new LeaderElectionJobListener() , JobNodePath.getNodeFullPath(jobName, ElectionNode.LEADER_HOST));
    }

    @Override
    public void close() throws Exception {
        isShutDown = false;
        leaderElectionService.close();
        zkCacheManager.closeNodeCache(JobNodePath.getNodeFullPath(jobName , ElectionNode.LEADER_HOST));
    }

    /**
     * leader选举的监听器
     * 所有follow进行监听 leader失效后，进行leader选举
     */
    class LeaderElectionJobListener implements NodeCacheListener{
        @Override
        public void nodeChanged() throws Exception {
            zkCacheManager.getExecutorService().execute(()->{
                try {
                  LOGGER.error(jobName , "Leader host nodeChanged", jobName);
                    if (isShutDown) {
                        LOGGER.debug( jobName, "ElectionListenerManager has been shutdown");
                        return;
                    }
                    if (!leaderElectionService.hasLeader()) {
                        LOGGER.info( jobName, "Leader crashed, elect a new leader now");
                        leaderElectionService.leaderElection();
                        LOGGER.info( jobName, "Leader election completed");
                    } else {
                        LOGGER.debug(jobName, "Leader is already existing, unnecessary to election");
                    }
                }catch (Throwable t){
                    LOGGER.error(jobName , t.getMessage() ,t);
                }
            });
        }
    }
}
