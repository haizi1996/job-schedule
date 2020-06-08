package com.hailin.job.schedule.core.executor;

import com.hailin.shrine.job.common.exception.TimeDiffIntolerableException;
import com.hailin.shrine.job.common.util.LocalHostService;
import com.hailin.shrine.job.common.util.LogEvents;
import com.hailin.shrine.job.common.util.ResourceUtils;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 任务执行服务
 * @author zhanghailin
 */
public class ScheduleExecutorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleExecutorService.class);

    public static final int WAIT_JOBCLASS_ADDED_COUNT = 25 ;

    private static final int WAIT_FOR_IP_NODE_DISAPPEAR_COUNT = 150;

    private String executorName ;

    private String executorVersion;

    private CoordinatorRegistryCenter coordinatorRegistryCenter;

    private String ipNode;

    private ClassLoader jobClassLoader;

    private ClassLoader executorClassLoader;

    private ScheduleExecutorExtension scheduleExecutorExtension;

    private InitNewJobService initNewJobService;
    private ExecutorConfigService executorConfigService ;

    private RestartAndDumpService restartExecutorService;

    private Object scheduleApplication;

    public ScheduleExecutorService( CoordinatorRegistryCenter coordinatorRegistryCenter , String executorName, ScheduleExecutorExtension scheduleExecutorExtension) {
        this.executorName = executorName;
        this.coordinatorRegistryCenter = coordinatorRegistryCenter;
        this.scheduleExecutorExtension = scheduleExecutorExtension;

        if (coordinatorRegistryCenter != null){
            coordinatorRegistryCenter.setExecutorName(executorName);
        }
    }

    /**
     * 注册Executor
     */
    public void registerExecutor()throws Exception{
        checkExecutor();
        registerExecutor0();
    }

    public void unregisterExecutor(){
        stopRestartExecutorService();
        stopExecutorConfigService();
        removeIpNode();
    }

    private void removeIpNode() {
        try {
            if (coordinatorRegistryCenter != null){
                LOGGER.info( LogEvents.ExecutorEvent.SHUTDOWN, "{} is going to delete its ip node {}",
                        executorName, ipNode);
                coordinatorRegistryCenter.remove(ipNode);
            }
        }catch (Throwable throwable){
            LOGGER.error(LogEvents.ExecutorEvent.INIT , throwable.toString() , throwable);
        }
    }

    private void stopExecutorConfigService() {
        try {
            if (executorConfigService != null){
                executorConfigService.stop();
            }
        }catch (Throwable throwable){
            LOGGER.error(LogEvents.ExecutorEvent.INIT , throwable.toString() , throwable);
        }
    }

    private void stopRestartExecutorService() {
        try {
            if (restartExecutorService != null){
                restartExecutorService.stop();
            }
        }catch (Throwable throwable){
            LOGGER.error(LogEvents.ExecutorEvent.INIT , throwable.toString() , throwable);
        }
    }


    public void removeJobName(String jobName){
        if (initNewJobService != null){
            initNewJobService.removeJobName(jobName);
        }
    }

    private void registerExecutor0() throws Exception{
        String executorNode = ShrineExecutorsNode.EXECUTORS_ROOT + "/" + executorName;
        ipNode = executorNode + "/ip";
        String lastBeginTimeNode = executorNode + "/lastBeginTime";
        String versionNode = executorNode + "/version";
        String executorCleanNode = executorNode + "/clean";
        String executorTaskNode = executorNode + "/task";

        // 持久化最近启动时间
        coordinatorRegistryCenter.persist(lastBeginTimeNode, String.valueOf(System.currentTimeMillis()));
        // 持久化版本
        executorVersion = getExecutorVersionFromFile();
        if (executorVersion != null) {
            coordinatorRegistryCenter.persist(versionNode, executorVersion);
        }

        // 持久化clean
        coordinatorRegistryCenter
                .persist(executorCleanNode, String.valueOf(SystemEnvProperties.SHRINE_EXECUTOR_CLEAN));

        // 持久task
        if (StringUtils.isNotBlank(SystemEnvProperties.SHRINE_CONTAINER_DEPLOYMENT_ID)) {
            LOGGER.info(LogEvents.ExecutorEvent.INIT, "persist znode '/task': {}",
                    SystemEnvProperties.SHRINE_CONTAINER_DEPLOYMENT_ID);
            coordinatorRegistryCenter.persist(executorTaskNode, SystemEnvProperties.SHRINE_CONTAINER_DEPLOYMENT_ID);
        }

        // 获取配置并添加watcher
        if (executorConfigService != null) {
            executorConfigService.stop();
        }
        executorConfigService = new ExecutorConfigService(executorName,
                (CuratorFramework) coordinatorRegistryCenter.getRawClient(),
                scheduleExecutorExtension.getExecutorConfigClass());
        executorConfigService.start();

        // add watcher for restart
        if (restartExecutorService != null) {
            restartExecutorService.stop();
        }
        restartExecutorService = new RestartAndDumpService(executorName, coordinatorRegistryCenter);
        restartExecutorService.start();

        // 持久化ip
        coordinatorRegistryCenter.persistEphemeral(ipNode, LocalHostService.cachedIpAddress);
    }

    private String getExecutorVersionFromFile() {
        try {
            Properties props = ResourceUtils.getResource("properties/shrine-core.properties");
            if (props != null) {
                String version = props.getProperty("build.version");
                if (!StringUtils.isBlank(version)) {
                    return version.trim();
                } else {
                    LOGGER.error(LogEvents.ExecutorEvent.INIT, "the build.version property is not existing");
                }
            } else {
                LOGGER.error( LogEvents.ExecutorEvent.INIT, "the saturn-core.properties file is not existing");
            }
            return null;
        }catch (IOException e){
            LOGGER.error(LogEvents.ExecutorEvent.INIT ,e.toString() , e);
            return null;
        }
    }

    /**
     * 启动前先检查本机与注册中心的时间误差秒数是否在允许范围和Executor是否已经启动
     */
    private void checkExecutor() throws Exception{
        String executorNode = ShrineExecutorsNode.EXECUTORS_ROOT + "/" + executorName;
        try {
            long timeDiff = Math.abs(System.currentTimeMillis() - coordinatorRegistryCenter
                    .getRegistryCenterTime(executorNode + "/systemTime/current"));
            int maxTimeDiffSeconds = 60;
            if (timeDiff > maxTimeDiffSeconds){
                Long timeDiffSeconds = Long.valueOf(timeDiff / 1000);
                throw new TimeDiffIntolerableException(timeDiffSeconds.intValue() , maxTimeDiffSeconds);
            }
        }finally {
            String executorSystemTimePath = executorNode + "/systemTime";
            if (coordinatorRegistryCenter.isExisted(executorSystemTimePath)) {
                coordinatorRegistryCenter.remove(executorSystemTimePath);
            }
        }
        //启动时检查Executor是否已启用（ExecutorName为判断的唯一标识）
        if (coordinatorRegistryCenter.isExisted(executorNode)){
            int count = 0;
            do {
                if (!coordinatorRegistryCenter.isExisted(executorNode + "/ip")){
                    return;
                }
                LOGGER.warn(LogEvents.ExecutorEvent.INIT , "{}/ip node found. Try to sleep and wait for this node disappear.", executorNode);
                Thread.sleep(100L);
            }while (++ count <= WAIT_FOR_IP_NODE_DISAPPEAR_COUNT);
            throw new Exception("The executor (\" + executorName + \") is running, cannot running the instance twice.");
        }else {
            coordinatorRegistryCenter.persist(executorNode , StringUtils.EMPTY);
        }
    }
    /**
     * 注册$/Jobs的watcher，响应添加作业的事件，初始化作业。注意，会响应已经存在的作业。
     */
    public void registerJobsWatcher() throws Exception{
        if (initNewJobService != null){
            initNewJobService.shutdown();
        }
        initNewJobService = new InitNewJobService(this);
        initNewJobService.start();
    }
    /**
     * 销毁监听添加作业的watcher。关闭正在初始化作业的线程，直到其结束。
     */
    public void unregisterJobsWatcher() {
        if (initNewJobService != null) {
            initNewJobService.shutdown();
        }
    }



    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public String getExecutorVersion() {
        return executorVersion;
    }

    public void setExecutorVersion(String executorVersion) {
        this.executorVersion = executorVersion;
    }

    public CoordinatorRegistryCenter getCoordinatorRegistryCenter() {
        return coordinatorRegistryCenter;
    }

    public void setCoordinatorRegistryCenter(CoordinatorRegistryCenter coordinatorRegistryCenter) {
        this.coordinatorRegistryCenter = coordinatorRegistryCenter;
    }

    public String getIpNode() {
        return ipNode;
    }

    public void setIpNode(String ipNode) {
        this.ipNode = ipNode;
    }

    public ClassLoader getJobClassLoader() {
        return jobClassLoader;
    }

    public void setJobClassLoader(ClassLoader jobClassLoader) {
        this.jobClassLoader = jobClassLoader;
    }

    public ClassLoader getExecutorClassLoader() {
        return executorClassLoader;
    }

    public void setExecutorClassLoader(ClassLoader executorClassLoader) {
        this.executorClassLoader = executorClassLoader;
    }

    public ScheduleExecutorService getScheduleExecutorService() {
        return scheduleExecutorService;
    }

    public void setScheduleExecutorService(ScheduleExecutorService scheduleExecutorService) {
        this.scheduleExecutorService = scheduleExecutorService;
    }

    public Object getScheduleApplication() {
        return scheduleApplication;
    }

    public void setScheduleApplication(Object scheduleApplication) {
        this.scheduleApplication = scheduleApplication;
    }
}
