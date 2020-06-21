package com.hailin.job.schedule.core.basic.statistics;

import com.hailin.job.schedule.core.basic.analyse.AnalyseService;
import com.hailin.job.schedule.core.basic.server.ServerService;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.strategy.JobScheduler;

/**
 * 统计处理数据数量的作业
 */
public class ProcessCountJob implements Runnable {

    private final JobConfiguration jobConfiguration;

    private final ServerService serverService;

    private final AnalyseService analyseService;

    public ProcessCountJob(final JobScheduler jobScheduler) {
        jobConfiguration = jobScheduler.getCurrentConf();
        serverService = jobScheduler.getServerService();
        analyseService = jobScheduler.getAnalyseService();
    }
//
    @Override
    public void run() {
        String jobName = jobConfiguration.getJobName();
        serverService.persistProcessSuccessCount(ProcessCountStatistics.getProcessSuccessCount(serverService.getExecutorName() , jobName));
        serverService.persistProcessFailureCount(ProcessCountStatistics.getProcessFailureCount(serverService.getExecutorName() , jobName));
        analyseService.persistTotalCount();
        analyseService.persistErrorCount();
    }
}
