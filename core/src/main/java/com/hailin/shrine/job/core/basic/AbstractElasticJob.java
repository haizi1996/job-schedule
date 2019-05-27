package com.hailin.shrine.job.core.basic;

import com.hailin.shrine.job.common.exception.JobException;
import com.hailin.shrine.job.core.basic.control.ReportService;
import com.hailin.shrine.job.core.basic.execution.ExecutionContextService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.failover.FailoverService;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.shrine.job.core.executor.ShrineExecutorService;
import com.hailin.shrine.job.core.job.trigger.ShrineScheduler;
import com.hailin.shrine.job.core.job.trigger.Trigger;
import com.hailin.shrine.job.core.job.trigger.Triggered;
import com.hailin.shrine.job.core.service.ConfigurationService;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;


/**
 * 弹性化分布式作业的基类
 * @author zhanghailin
 */
public abstract class AbstractElasticJob implements Stoppable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractElasticJob.class);

    //状态变量
    protected volatile boolean stopped = false;
    protected volatile boolean forceStopped = false;
    protected volatile boolean aborted = false;
    protected volatile boolean running = false;

    protected ConfigurationService configurationService;

    protected String jobVersion ;


    protected ShardingService shardingService;

    protected ExecutionContextService executionContextService;

    protected ExecutionService executionService;

    protected FailoverService failoverService;

    protected ServerService serverService;


    //区分不同业务系统
    protected String namespace;

    protected String executorName;

    protected String jobName;

    protected JobScheduler jobScheduler;

    protected ShrineScheduler scheduler;

    protected ShrineExecutorService shrineExecutorService;

    protected ReportService reportService;


    /**
     * 重置作业 调用一次周期的变量
     */
    private void reset(){
        stopped = false;
        forceStopped = false;
        aborted = false;
        running = true;
    }


    @Override
    public void shutdown() {

    }

    public ConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     *  进行任务执行
     * @param currentTriggered 当前执行数据
     */
    public final void execute(Triggered currentTriggered) {
        LOGGER.debug(jobName, "Saturn start to execute job [{}]", jobName);
        // 对每一个jobScheduler，作业对象只有一份，多次使用，所以每次开始执行前先要reset
        reset();
        if (configurationService == null){
            LOGGER.warn(jobName, "configService is null");
            return;
        }
        JobExecutionMultipleShardingContext shardingContext = null;

    }
    protected boolean mayRunDownStream(final JobExecutionMultipleShardingContext shardingContext) {
        return true;
    }


    protected abstract void executeJob(final JobExecutionMultipleShardingContext shardingContext);

    public void callbackWhenShardingItemIsEmpty(final JobExecutionMultipleShardingContext shardingContext) {
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public void forceStop() {
        forceStopped = true;
    }

    @Override
    public void abort() {
        aborted = true;
    }

    @Override
    public void resume() {
        stopped = false;
    }

    public void enableJob() {
        scheduler.getTrigger().enabledJob();
    }

    public void disableJob() {
        scheduler.getTrigger().disableJob();
    }

    public void onResharding() {
        scheduler.getTrigger().onReSharding();
    }

    public boolean isFailoverSupported() {
        return scheduler.getTrigger().isFailoverSupported();
    }

    public abstract void onForceStop(int item);

    public abstract void onTimeout(int item);

    public abstract void onNeedRaiseAlarm(int item, String alarmMessage);

    public void notifyJobEnabled() {
    }

    public void notifyJobDisabled() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public boolean isForceStopped() {
        return forceStopped;
    }

    public void setForceStopped(boolean forceStopped) {
        this.forceStopped = forceStopped;
    }

    public boolean isAborted() {
        return aborted;
    }

    public void setAborted(boolean aborted) {
        this.aborted = aborted;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public String getJobVersion() {
        return jobVersion;
    }

    public void setJobVersion(String jobVersion) {
        this.jobVersion = jobVersion;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public ShardingService getShardingService() {
        return shardingService;
    }

    public void setShardingService(ShardingService shardingService) {
        this.shardingService = shardingService;
    }

    public ExecutionContextService getExecutionContextService() {
        return executionContextService;
    }

    public void setExecutionContextService(ExecutionContextService executionContextService) {
        this.executionContextService = executionContextService;
    }



    public ExecutionService getExecutionService() {
        return executionService;
    }

    public void setExecutionService(ExecutionService executionService) {
        this.executionService = executionService;
    }

    public FailoverService getFailoverService() {
        return failoverService;
    }

    public void setFailoverService(FailoverService failoverService) {
        this.failoverService = failoverService;
    }

    public ServerService getServerService() {
        return serverService;
    }

    public void setServerService(ServerService serverService) {
        this.serverService = serverService;
    }

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public void setJobScheduler(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    public ShrineScheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(ShrineScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public ShrineExecutorService getShrineExecutorService() {
        return shrineExecutorService;
    }

    public void setShrineExecutorService(ShrineExecutorService shrineExecutorService) {
        this.shrineExecutorService = shrineExecutorService;
    }

    public ReportService getReportService() {
        return reportService;
    }

    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }


}
