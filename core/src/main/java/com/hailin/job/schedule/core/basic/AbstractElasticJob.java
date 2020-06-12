package com.hailin.job.schedule.core.basic;

import com.hailin.job.schedule.core.basic.control.ReportService;
import com.hailin.job.schedule.core.basic.execution.ExecutionContextService;
import com.hailin.job.schedule.core.basic.execution.ExecutionService;
import com.hailin.job.schedule.core.basic.failover.FailoverService;
import com.hailin.job.schedule.core.basic.server.ServerService;
import com.hailin.job.schedule.core.basic.sharding.ShardingService;
import com.hailin.job.schedule.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.job.schedule.core.config.JobProperties;
import com.hailin.job.schedule.core.executor.ScheduleExecutorService;
import com.hailin.job.schedule.core.handler.ExecutorServiceHandler;
import com.hailin.job.schedule.core.handler.ExecutorServiceHandlerRegistry;
import com.hailin.job.schedule.core.job.trigger.Trigger;
import com.hailin.job.schedule.core.service.ConfigurationService;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import com.hailin.job.schedule.core.job.trigger.ShrineScheduler;
import com.hailin.job.schedule.core.job.trigger.Triggered;
import com.hailin.shrine.job.common.exception.JobException;
import com.hailin.shrine.job.common.exception.JobSystemException;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;


/**
 * 弹性化分布式作业的基类
 * @author zhanghailin
 */
@Data
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

    protected ScheduleExecutorService scheduleExecutorService;

    protected ReportService reportService;

    protected ExecutorService executorService;



    public void init() {
        Class<? extends Trigger> triggerClass = configurationService.getJobType().getTriggerClass();

        Trigger trigger = null;
        try{
            trigger = triggerClass.newInstance();
            trigger.init(this);
        }catch (Exception e){
            LOGGER.error( "{} Trigger init failed " , jobName , e);
            throw new JobException(e);
        }
        executorService = ExecutorServiceHandlerRegistry.getExecutorServiceHandler(jobName, (ExecutorServiceHandler) getHandler(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER));

        scheduler = new ShrineScheduler(this , trigger);
        scheduler.start();
    }

    /**
     * 重置作业 调用一次周期的变量
     */
    private void reset(){
        stopped = false;
        forceStopped = false;
        aborted = false;
        running = true;
    }

    private Object getHandler(final JobProperties.JobPropertiesEnum jobPropertiesEnum) {
        String handlerClassName = jobRootConfig.getTypeConfig().getCoreConfig().getJobProperties().get(jobPropertiesEnum);
        try {
            Class<?> handlerClass = Class.forName(handlerClassName);
            if (jobPropertiesEnum.getClassType().isAssignableFrom(handlerClass)) {
                return handlerClass.newInstance();
            }
            return getDefaultHandler(jobPropertiesEnum, handlerClassName);
        } catch (final ReflectiveOperationException ex) {
            return getDefaultHandler(jobPropertiesEnum, handlerClassName);
        }
    }

    private Object getDefaultHandler(final JobProperties.JobPropertiesEnum jobPropertiesEnum, final String handlerClassName) {
        LOGGER.warn("Cannot instantiation class '{}', use default '{}' class.", handlerClassName, jobPropertiesEnum.getKey());
        try {
            return Class.forName(jobPropertiesEnum.getDefaultValue()).newInstance();
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new JobSystemException(e);
        }
    }

    @Override
    public void shutdown() {

    }

    public ExecutorService getExecutorService() {
        return jobScheduler.getExecutorService();
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
     */
    public final void execute() {
        LOGGER.debug(jobName, "Saturn start to execute job [{}]", jobName);
        // 对每一个jobScheduler，作业对象只有一份，多次使用，所以每次开始执行前先要reset
        reset();
        if (configurationService == null){
            LOGGER.warn(jobName, "configService is null");
            return;
        }
        JobExecutionMultipleShardingContext shardingContext = null;
        try {
            if (failoverService.getLocalHostFailoverItems().isEmpty()){
                shardingService.shardingIfNecessary();
            }
            if (!configurationService.isJobEnabled()){
                LOGGER.debug( "{} is disabled, cannot be continued, do nothing about business.",
                        jobName);
                return;
            }

            shardingContext = executionContextService.getJobExecutionShardingContext();
            if (CollectionUtils.isEmpty(shardingContext.getShardingItems())){
                LOGGER.debug(  "{} 's items of the executor is empty, do nothing about business.",
                        jobName);
                callbackWhenShardingItemIsEmpty(shardingContext);
                return;
            }

            if (configurationService.isInPausePeriod()) {
                LOGGER.info("the job {} current running time is in pausePeriod, do nothing about business.", jobName);
                return;
            }
            executeJobInternal(shardingContext);
            if (isFailoverSupported() && configurationService.isFailover() && !stopped && !forceStopped && !aborted){
                failoverService.failoverIfNecessary();
            }
            LOGGER.debug( "Saturn finish to execute job [{}], sharding context:{}.", jobName,
                    shardingContext);

        }catch (Exception e){
            LOGGER.warn(e.getMessage() + " " +jobName , e);
        }finally {
            running = false;
        }

    }

    private void executeJobInternal(JobExecutionMultipleShardingContext shardingContext){
        //  注册任务开始
        executionService.registerJobBegin(shardingContext);

        try {
            executeJob(shardingContext);
        }finally {
            List<Integer> shardingItems = shardingContext.getShardingItems();

            if (!shardingItems.isEmpty()){
                Date nextFireTimePausePeriodEffected = jobScheduler.getNextFireTimePausePeriodEffected();
                boolean isEnabledReport = configurationService.isEnabledReport();
                for (int item : shardingItems) {
                    if (isEnabledReport && !checkIfZkLostAfterExecution(item)) {
                        continue;
                    }
                    if (!aborted) {
                        executionService.registerJobCompletedByItem(shardingContext, item, nextFireTimePausePeriodEffected);
                    }
                    if (isFailoverSupported() && configService.isFailover()) {
                        failoverService.updateFailoverComplete(item);
                    }
                }
            }

        }
    }

    /**
     * 如果不存在该分片的running节点，有不是关闭了enabledReport的话，不能继续执行
     * @param item
     * @return
     */
    private boolean checkIfZkLostAfterExecution(int item){

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

    public ScheduleExecutorService getScheduleExecutorService() {
        return scheduleExecutorService;
    }

    public void setScheduleExecutorService(ScheduleExecutorService scheduleExecutorService) {
        this.scheduleExecutorService = scheduleExecutorService;
    }

    public ReportService getReportService() {
        return reportService;
    }

    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }


}
