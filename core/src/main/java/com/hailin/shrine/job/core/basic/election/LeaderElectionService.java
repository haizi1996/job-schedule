package com.hailin.shrine.job.core.basic.election;

import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.storage.JobNodeStorage;
import com.hailin.shrine.job.core.basic.storage.LeaderExecutionCallback;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 选举主节点的服务类
 * @author zhanghailin
 */
public class LeaderElectionService extends AbstractShrineService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionService.class);

    private AtomicBoolean shutDown = new AtomicBoolean( false);

    public LeaderElectionService(final JobScheduler jobScheduler){
        super(jobScheduler);
    }


    @Override
    public void close() throws Exception {
        synchronized (shutDown){
            if (shutDown.compareAndSet(false ,true)){
                try {
                    JobNodeStorage jobNodeStorage = getJobNodeStorage();

                    if(jobNodeStorage.isConnected() &&
                    executorName.equals(jobNodeStorage.getJobNodeDataDirectly(ElectionNode.LEADER_HOST))){
                        jobNodeStorage.removeJobNodeIfExisted(ElectionNode.LEADER_HOST);
                        LOGGER.info("{} that was {}'s leader, released itself", executorName, jobName);
                    }
                }catch (Throwable t){
                    LOGGER.error( jobName, t.getMessage(), t);
                }
            }
        }
    }

    /**
     * 选举主节点
     */
    public void leaderElection(){
        getJobNodeStorage().executeInLeader(ElectionNode.LATCH , new LeaderElectionCallback());
    }

    /**
     * 判断当前节点是否是主节点
     * 如果没有主节点，则选举，知道有主节点
     */
    public Boolean isLeader(){
        while (!shutDown.get() && !hasLeader()){
            LOGGER.info( jobName, "No leader, try to election");
            leaderElection();
        }
        return executorName.equals(getJobNodeStorage().getJobNodeDataDirectly(ElectionNode.LEADER_HOST));
    }

    /**
     * 判断是否已经有主节点
     */
    public boolean hasLeader(){
        return getJobNodeStorage().isJobNodeExisted(ElectionNode.LEADER_HOST);
    }

    class LeaderElectionCallback implements LeaderExecutionCallback{
        @Override
        public void execute() {
            synchronized (shutDown){
                if (shutDown.get()){
                    return;
                }
                if (!getJobNodeStorage().isJobNodeExisted(ElectionNode.LEADER_HOST)){
                    getJobNodeStorage().fillEphemeralJobNode(ElectionNode.LEADER_HOST, executorName);
                    LOGGER.info(jobName, "executor {} become job {}'s leader", executorName, jobName);
                }
            }
        }
    }
}
