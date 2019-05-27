package com.hailin.shrine.job.core.strategy;

import com.google.common.base.Optional;
import com.hailin.shrine.job.common.exception.JobConfigurationException;
import com.hailin.shrine.job.common.exception.JobSystemException;
import com.hailin.shrine.job.core.basic.AbstractElasticJob;
import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.analyse.AnalyseService;
import com.hailin.shrine.job.core.basic.control.ReportService;
import com.hailin.shrine.job.core.basic.election.LeaderElectionService;
import com.hailin.shrine.job.core.basic.execution.ExecutionContextService;
import com.hailin.shrine.job.core.basic.execution.ExecutionService;
import com.hailin.shrine.job.core.basic.failover.FailoverService;
import com.hailin.shrine.job.core.config.JobConfiguration;
import com.hailin.shrine.job.core.listener.ListenerManager;
import com.hailin.shrine.job.core.basic.schdule.SchedulerFacade;
import com.hailin.shrine.job.core.basic.schdule.ShrineJobFacade;
import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.sharding.ShardingService;
import com.hailin.shrine.job.core.basic.statistics.StatisticsService;
import com.hailin.shrine.job.core.basic.storage.JobNodeStorage;
import com.hailin.shrine.job.core.basic.threads.ExtendableThreadPoolExecutor;
import com.hailin.shrine.job.core.basic.threads.ShrineThreadFactory;
import com.hailin.shrine.job.core.basic.threads.TaskQueue;
import com.hailin.shrine.job.core.executor.LimitMaxJobService;
import com.hailin.shrine.job.core.executor.ShrineExecutorService;
import com.hailin.shrine.job.core.job.JobFacade;
import com.hailin.shrine.job.core.job.trigger.ShrineScheduler;
import com.hailin.shrine.job.core.job.type.script.ScriptJob;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.reg.zookeeper.ZkCacheManager;
import com.hailin.shrine.job.core.schedule.JobScheduleController;
import com.hailin.shrine.job.core.schedule.JobShutdownHookPlugin;
import com.hailin.shrine.job.core.schedule.ShrineJob;
import com.hailin.shrine.job.core.service.ConfigurationService;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 作业调度器
 * @author zhanghailin
 */
@Setter
public class JobScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobScheduler.class);

    public static final String SHRINE_JOB_DATA_MAP_KEY = "shrineJob";

    private static final String JOB_FACADE_DATA_MAP_KEY = "jobFacade";


    private ExecutorService executorService;

    private final JobConfiguration jobConfiguration;

    private final CoordinatorRegistryCenter regCenter;

    private  JobConfiguration currentConf;

    private final JobNodeStorage jobNodeStorage;

    //作业名称
    private String jobName ;

    //执行器的名称
    private String executorName;

    private final ZkCacheManager zkCacheManager;

    private JobConfiguration previousConfig = new JobConfiguration(null , null);

    private final ConfigurationService configService;


    private AbstractElasticJob job;

    private ShrineExecutorService shrineExecutorService;

    private ExecutionContextService executionContextService;

    private LeaderElectionService leaderElectionService ;

    private ReportService reportService;

    private ServerService serverService;


    private ShardingService shardingService;

    private FailoverService failoverService;

    private ExecutionService executionService;

    private StatisticsService statisticsService;

    private AnalyseService analyseService;

    private AtomicBoolean isShutdownFlag = new AtomicBoolean(false);

    private ListenerManager listenerManager;

    private LimitMaxJobService limitMaxJobService ;

    private SchedulerFacade schedulerFacade;
    private final JobFacade jobFacade;

    public JobScheduler(JobConfiguration jobConfig, CoordinatorRegistryCenter regCenter) {
        this.jobConfiguration = jobConfig;
        this.regCenter = regCenter;
        this.jobNodeStorage = new JobNodeStorage(regCenter , jobConfig);
        zkCacheManager = new ZkCacheManager((CuratorFramework) regCenter.getRawClient(), jobName,
                executorName);
        configService= new ConfigurationService(currentConf.getJobName() ,regCenter);
        schedulerFacade = new SchedulerFacade( currentConf.getJobName() , regCenter);
        jobFacade = new ShrineJobFacade( currentConf.getJobName() , regCenter);

    }


    public void init() {
//        try {
//            startAll();
//            createJob();
//            serverService.persistServerOnline(job);
//            configService.notifyJobEnabledOrNot();
//        }catch (Throwable throwable){
//            shutdown(false);
//            throw throwable;
//        }
        JobConfiguration jobConfigFromRegCenter = schedulerFacade.updateJobConfiguration(jobConfiguration);
        JobRegistry.getInstance().setCurrentShardingTotalCount(jobConfigFromRegCenter.getJobName() , jobConfigFromRegCenter.getShardingTotalCount());
        JobScheduleController jobScheduleController = new JobScheduleController(createScheduler(), createJobDetail(currentConf.getTypeConfig().getJobClass()), currentConf.getJobName());
        schedulerFacade.registerStartUpInfo(jobConfigFromRegCenter.isEnabled());
        JobRegistry.getInstance().registerJob(jobName ,jobScheduleController,regCenter );
//        jobScheduleController.scheduleJob(jobConfigFromRegCenter.getTypeConfig().getCoreConfig().getCron());
    }

//    private void createJob() {
//        try{
//            job = JobTypeManager.get(currentConf.getJobType()).getHandlerClass().newInstance();
//        }catch (Exception e){
//            LOGGER.error(jobName , "unexpected error" , e);
//            throw new JobException(e);
//        }
//        job.setJobScheduler(this);
//        job.setConfigurationService(configService);
//        job.setShardingService(shardingService);
//        job.setExecutionContextService(executionContextService);
//        job.setExecutionService(executionService);
//        job.setFailoverService(failoverService);
//        job.setServerService(serverService);
//        job.setExecutorName(executorName);
//        job.setReportService(reportService);
//        job.setJobName(jobName);
//        job.setNamespace(regCenter.getNamespace());
//        job.setShrineExecutorService(shrineExecutorService);
//        job.init();
//    }

    private void startAll() {
        configService.start();
        leaderElectionService.start();
        serverService.start();
        shardingService.start();
        executionContextService.start();
        executionService.start();
        failoverService.start();
        statisticsService.start();
        limitMaxJobService.start();
        analyseService.start();

        limitMaxJobService.check(currentConf.getJobName());
        listenerManager.start();
        leaderElectionService.leaderElection();

        serverService.clearRunOneTimePath();
        serverService.clearStopOneTimePath();
        serverService.resetCount();
//        statisticsService.startProcesCountJob();
    }


    /**
     * 立刻启动此任务
     * @param triggeredDataStr 触发的时间字符串
     */
    public void triggerJob(String triggeredDataStr) {
        if (job.getScheduler().isShutdown()){
            return;
        }
        job.getScheduler().trigger(triggeredDataStr);

    }


    /**
     * 获取下次作业触发时间.可能被暂停时间段所影响。
     *
     * @return 下次作业触发时间
     */
    public Date getNextFireTimePausePeriodEffected() {
        try {
            ShrineScheduler saturnScheduler = job.getScheduler();
            return saturnScheduler == null ? null : saturnScheduler.getNextFireTimePausePeriodEffected();
        } catch (Throwable t) {
            LOGGER.error( jobName, "fail to get next fire time", t);
            return null;
        }
    }

    private JobDetail createJobDetail(final String jobClass) {
        JobDetail result = JobBuilder.newJob(ShrineJob.class).withIdentity(jobConfiguration.getJobName()).build();
        result.getJobDataMap().put(JOB_FACADE_DATA_MAP_KEY, jobFacade);
        Optional<ShrineJob> elasticJobInstance = createElasticJobInstance();
        if (elasticJobInstance.isPresent()) {
            result.getJobDataMap().put(SHRINE_JOB_DATA_MAP_KEY, elasticJobInstance.get());
        } else if (!jobClass.equals(ScriptJob.class.getCanonicalName())) {
            try {
                result.getJobDataMap().put(SHRINE_JOB_DATA_MAP_KEY, Class.forName(jobClass).newInstance());
            } catch (final ReflectiveOperationException ex) {
                throw new JobConfigurationException("Elastic-Job: Job class '%s' can not initialize.", jobClass);
            }
        }
        return result;
    }

    protected Optional<ShrineJob> createElasticJobInstance() {
       return Optional.absent();
    }

    public void shutdown( boolean removeJob){
        synchronized (isShutdownFlag){
            isShutdownFlag.set(true);

            //关闭Listener
            listenerManager.shutdown();
            //关闭作业
            if (job != null){
                job.shutdown();
            }
            //关闭服务
            shardingService.shutdown();
            configService.shutdown();
            leaderElectionService.shutdown();
            serverService.shutdown();
            executionContextService.shutdown();
            executionService.shutdown();
            failoverService.shutdown();
            statisticsService.shutdown();
            analyseService.shutdown();
            limitMaxJobService.shutdown();

            //关闭TreeCache
            zkCacheManager.shutdown();

            if (removeJob){
                jobNodeStorage.deleteJobNode();
                shrineExecutorService.removeJobName(jobName);
            }
            //移除作业注册表
//            JobRegistry.clearJob(executorName , jobName);
        }
    }
    public void reCreateExecutorService(){
        synchronized (isShutdownFlag){
            if (isShutdownFlag.get()){
                LOGGER.warn(jobName , "the jobScheduler was shutdown , cannot re-create business thread pool");
                return;
            }
            executionService.shutdown();
            initExecutorService();
        }
    }

    /**
     * 创建Quartz的调度器
     * @return
     */
    private Scheduler createScheduler() {
        Scheduler result;
        try {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            factory.initialize(getBaseQuartzProperties());
            result = factory.getScheduler();
            result.getListenerManager().addTriggerListener(schedulerFacade.newJobTriggerListener());
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
        return result;
    }

    private Properties getBaseQuartzProperties() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", org.quartz.simpl.SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "1");
        result.put("org.quartz.scheduler.instanceName", currentConf.getNameSpace());
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }

    private void initExecutorService() {
        ThreadFactory factory = new ShrineThreadFactory(jobName);
        executorService = new ExtendableThreadPoolExecutor(0 , 100 , 2 , TimeUnit.MICROSECONDS , new TaskQueue() , factory);
    }

    public void shutdownExecutorService(){
        if (executorService != null && !executorService.isShutdown()){
            executorService.shutdown();
        }
    }

    public void reInitializeTrigger(){
        job.getScheduler().reInitializeTrigger();
    }

    /**
     *     关闭统计线程
     */
    public void shutdownCountThread()  {
        statisticsService.shutdown();
    }

    /**
     * 重启统计处理数据数量的任务
     */
//    public void rescheduleProcessCountJob(){
//        statisticsService.startProcesCountJob();
//    }


    public ReportService getReportService() {
        return reportService;
    }

    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }

    public ExecutionContextService getExecutionContextService() {
        return executionContextService;
    }

    public void setExecutionContextService(ExecutionContextService executionContextService) {
        this.executionContextService = executionContextService;
    }

    public LeaderElectionService getLeaderElectionService() {
        return leaderElectionService;
    }

    public void setLeaderElectionService(LeaderElectionService leaderElectionService) {
        this.leaderElectionService = leaderElectionService;
    }

    public AbstractElasticJob getJob() {
        return job;
    }

    public void setJob(AbstractElasticJob job) {
        this.job = job;
    }




}
