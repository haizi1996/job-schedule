package com.hailin.shrine.job.core.basic.server;

import com.hailin.shrine.job.common.util.LocalHostService;
import com.hailin.shrine.job.core.basic.AbstractElasticJob;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.strategy.JobScheduler;

import java.util.Collections;
import java.util.List;

/**
 * 作业服务器节点服务
 * @author zhanghailin
 */
public class ServerService extends AbstractShrineService {

    private LeaderElectionService leaderElectionService;

    public ServerService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        leaderElectionService = jobScheduler.getLeaderElectionService();
    }

    /**
     * 持久化作业服务器上线相关信息
     */
    public void persistServerOnline(AbstractElasticJob job){
        if (!leaderElectionService.hasLeader()){
            leaderElectionService.leaderElection();
        }
        persistIp();
        persistVersion();
        persistJobVersion(job.getJobVersion());
        ephemeralPersistServerReady();
    }

    public void resetCount(){
        persistProcessFailureCount(0);
        persistProcessSuccessCount(0);
    }

    /**
     * 持久化统计处理数据失败的数量的数据
     * @param processFailureCount 失败数
     */
    public void persistProcessFailureCount(final int processFailureCount) {
        getJobNodeStorage().replaceJobNode(ServerNode.getProcessFailureCountNode(executorName) , processFailureCount);
    }

    private void persistVersion() {
        String executorVersion = null;//jobScheduler
        if (executorVersion != null){
            getJobNodeStorage().fillJobNodeIfNullOrOverwrite(ServerNode.getVersionNode(executorName) , executorVersion);
        }
    }

    private void persistIp() {
        getJobNodeStorage().fillJobNodeIfNullOrOverwrite(ServerNode.getIpNode(executorName),
                LocalHostService.cachedIpAddress);
    }

    private void persistJobVersion(String jobVersion) {
        if (jobVersion != null) {
            getJobNodeStorage().fillJobNodeIfNullOrOverwrite(ServerNode.getJobVersionNode(executorName), jobVersion);
        }
    }

    private void ephemeralPersistServerReady() {
        getJobNodeStorage().fillEphemeralJobNode(ServerNode.getStatusNode(executorName), "");
    }

    /**
     * 清除立即终止作业的标记
     */
    public void clearStopOneTimePath() {
        getJobNodeStorage().removeJobNodeIfExisted(ServerNode.getStopOneTimePath(executorName));
    }

    /**
     * 获取该作业的所有服务器列表.
     *
     * @return 所有的作业服务器列表
     */
    public List<String> getAllServers() {
        List<String> result = getJobNodeStorage().getJobNodeChildrenKeys(ServerNode.ROOT);
        Collections.sort(result);
        return result;
    }

    /**
     * 持久化统计处理数据成功的数量的数据.
     *
     * @param processSuccessCount 成功数
     */
    public void persistProcessSuccessCount(final int processSuccessCount) {
        getJobNodeStorage().replaceJobNode(ServerNode.getProcessSuccessCountNode(executorName), processSuccessCount);
    }
    /**
     * 清除立即运行的标记
     */
    public void clearRunOneTimePath() {
        getJobNodeStorage().removeJobNodeIfExisted(ServerNode.getRunOneTimePath(executorName));
    }
}
