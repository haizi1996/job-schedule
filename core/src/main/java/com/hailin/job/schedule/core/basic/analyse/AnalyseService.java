package com.hailin.job.schedule.core.basic.analyse;

import com.hailin.job.schedule.core.basic.statistics.ProcessCountStatistics;
import com.hailin.job.schedule.core.basic.AbstractShrineService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;

/**
 * 作业服务器节点统计服务
 * @author zhanghailin
 */
public class AnalyseService extends AbstractShrineService {

    public AnalyseService(String jobName, CoordinatorRegistryCenter coordinatorRegistryCenter) {
        super(jobName, coordinatorRegistryCenter);
    }

    @Override
    public void start(){
        super.start();
        initTotalCount();
        initErrorCount();
    }

    public synchronized void persistTotalCount() {
        int delta = ProcessCountStatistics.getTotalCountDelta(executorName, jobName);
        if (delta > 0) {
            int totalCount = Integer.parseInt(getJobNodeStorage().getJobNodeData(AnalyseNode.PROCESS_COUNT));
            getJobNodeStorage().updateJobNode(AnalyseNode.PROCESS_COUNT, totalCount + delta);
            ProcessCountStatistics.initTotalCountDelta(executorName, jobName, 0);
        }
    }

    public synchronized void persistErrorCount() {
        int delta = ProcessCountStatistics.getErrorCountDelta(executorName, jobName);
        if (delta > 0) {
            int errorCount = Integer.parseInt(getJobNodeStorage().getJobNodeData(AnalyseNode.ERROR_COUNT));
            getJobNodeStorage().updateJobNode(AnalyseNode.ERROR_COUNT, delta + errorCount);
            ProcessCountStatistics.initErrorCountDelta(executorName, jobName, 0);
        }
    }

    private void initErrorCount() {
        Integer totalCount = 0;
        if (!getJobNodeStorage().isJobNodeExisted(AnalyseNode.ERROR_COUNT)){
            getJobNodeStorage().createOrUpdateJobNodeWithValue(AnalyseNode.ERROR_COUNT , totalCount.toString());
        }
        ProcessCountStatistics.initTotalCountDelta(executorName , jobName, 0);

    }

    private void initTotalCount() {
        Integer totalCount = 0;
        if (!getJobNodeStorage().isJobNodeExisted(AnalyseNode.PROCESS_COUNT)){
            getJobNodeStorage().createOrUpdateJobNodeWithValue(AnalyseNode.PROCESS_COUNT , totalCount.toString());
        }
        ProcessCountStatistics.initTotalCountDelta(executorName , jobName, 0);
    }
}
