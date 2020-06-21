package com.hailin.job.schedule.core.basic.statistics;

import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 作业统计信息服务
 * @author zhanghailin
 */
public class StatisticsService extends AbstractScheduleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsService.class);

    private ConfigurationService configService;

    private ScheduledExecutorService processCountExecutor;

    private ScheduledFuture<?> processCountJobFuture;

    private boolean isDown = false;

    public StatisticsService(final JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    public synchronized void startProcesCountJob(){
        int processCountIntervalSeconds = configService.getProcessCountIntervalSeconds();
        if (processCountIntervalSeconds > 0){
            if (processCountJobFuture != null){
                processCountJobFuture.cancel(true);
                LOGGER.info( jobName,
                        "Reschedule ProcessCountJob of the {} job, the processCountIntervalSeconds is {}",
                        jobConfiguration.getJobName(), processCountIntervalSeconds);
                processCountJobFuture = processCountExecutor
                        .scheduleAtFixedRate(new ProcessCountJob(jobScheduler), new Random().nextInt(10),
                                processCountIntervalSeconds, TimeUnit.SECONDS);
            }
        }else {
            if (processCountJobFuture != null) {
                LOGGER.info( jobName, "shutdown the task of reporting statistics data");
                processCountJobFuture.cancel(true);
                processCountJobFuture = null;
            }
        }
    }

    @Override
    public synchronized void start() {
        configService = jobScheduler.getConfigService();
        processCountExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                private AtomicInteger number = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    StringBuilder name = new StringBuilder(jobConfiguration.getJobName()).append("-ProcessCount-Thread-")
                            .append(number.incrementAndGet());
                    Thread t = new Thread(r, name.toString());
                    if (t.isDaemon()) {
                        t.setDaemon(false);
                    }
                    if (t.getPriority() != Thread.NORM_PRIORITY) {
                        t.setPriority(Thread.NORM_PRIORITY);
                    }
                    return t;
            }
        });
    }

    /**
     * 停止统计处理数据数量的作业
     */
    public synchronized void stopProcessCountJob(){
        if (processCountJobFuture != null){
            processCountJobFuture.cancel(true);
        }
        if (processCountExecutor != null){
            processCountExecutor.shutdown();
        }
    }

    @Override
    public void shutdown() {
        if (isDown){
            return;
        }
        isDown = true;
        stopProcessCountJob();
        ProcessCountStatistics.resetSuccessFailureCount(executorName, jobName);
    }

    /**
     * 开启或重启统计处理数据数量的作业.
     */
    public synchronized void startProcessCountJob() {
        int processCountIntervalSeconds = configService.getProcessCountIntervalSeconds();
        if (processCountIntervalSeconds > 0) {

            if (processCountJobFuture != null) {
                processCountJobFuture.cancel(true);
                LOGGER.info("Reschedule ProcessCountJob of the {} job, the processCountIntervalSeconds is {}",
                        jobConfiguration.getJobName(), processCountIntervalSeconds);
            }
            processCountJobFuture = processCountExecutor
                    .scheduleAtFixedRate(new ProcessCountJob(jobScheduler), new Random().nextInt(10),
                            processCountIntervalSeconds, TimeUnit.SECONDS);

        } else { // don't count, reset to zero.
            if (processCountJobFuture != null) {
                LOGGER.info("shutdown the task of reporting statistics data");
                processCountJobFuture.cancel(true);
                processCountJobFuture = null;
            }
        }
    }

}
