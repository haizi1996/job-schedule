package com.hailin.job.schedule.core.basic.election;

import com.hailin.job.schedule.core.strategy.JobScheduler;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.basic.storage.JobNodeStorage;
import com.hailin.job.schedule.core.basic.storage.LeaderExecutionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 选举主节点的服务类
 * @author zhanghailin
 */
public class LeaderElectionService extends AbstractScheduleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionService.class);

    private AtomicBoolean shutDown = new AtomicBoolean( false);


    public LeaderElectionService(final JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    /**
     * 删除主节点供重新选举.
     */
    public void removeLeader() {
        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }
    @Override
    public void shutdown()  {
        synchronized (shutDown){
            if (shutDown.compareAndSet(false ,true)){
                try {
                    JobNodeStorage jobNodeStorage = getJobNodeStorage();

                    if(jobNodeStorage.isConnected() &&
                    executorName.equals(jobNodeStorage.getJobNodeDataDirectly(LeaderNode.INSTANCE))){
                        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
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
        getJobNodeStorage().executeInLeader(LeaderNode.LATCH , new LeaderElectionCallback());
    }


    /**
     * 判断当前节点是否是主节点.
     *
     * @return 当前节点是否是主节点
     */
    public boolean isLeader() {
        while(!shutDown.get() && !hasLeader()){
            LOGGER.info( "No leader, try to election {}" , jobName);
            leaderElection();
        }
        return executorName.equals(getJobNodeStorage().getJobNodeDataDirectly(ElectionNode.LEADER_HOST));
    }

    /**
     * 判断是否已经有主节点
     */
    public boolean hasLeader(){
        return getJobNodeStorage().isJobNodeExisted(LeaderNode.INSTANCE);
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
