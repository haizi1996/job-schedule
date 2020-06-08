package com.hailin.job.schedule.core.executor;

import com.google.common.collect.Maps;
import com.hailin.shrine.job.common.util.LogEvents;
import com.hailin.job.schedule.core.basic.storage.JobNodePath;
import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * 初始化新任务的服务
 * @author zhanghailin
 */
public class InitNewJobService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitNewJobService.class);

    /**
     * record the alarm message hashcode, permanently saved, used for just raising alarm one time for one type exception
     */
    private static final ConcurrentMap<String, ConcurrentMap<String, Set<Integer>>> JOB_INIT_FAILED_RECORDS = new ConcurrentHashMap<>();
    private ScheduleExecutorService scheduleExecutorService;
    private String executorName;
    private CoordinatorRegistryCenter regCenter;
    private TreeCache treeCache;
    private ExecutorService executorService;
    private List<String> jobNames = new ArrayList<>();


    public InitNewJobService(ScheduleExecutorService scheduleExecutorService) {
        this.scheduleExecutorService = scheduleExecutorService;
        this.executorName = scheduleExecutorService.getExecutorName();
        this.regCenter = scheduleExecutorService.getCoordinatorRegistryCenter();
        JOB_INIT_FAILED_RECORDS.putIfAbsent(executorName , Maps.newConcurrentMap());
    }

    public void start()throws Exception{
        treeCache  = TreeCache.newBuilder((CuratorFramework) regCenter.getRawClient() , JobNodePath.ROOT)
                .setExecutor(Executors.newSingleThreadExecutor(new ScheduleThreadFactory(executorName + "-$Job-watcher" , false)) )
                .setMaxDepth(1).build();
        executorService = Executors.newSingleThreadExecutor(new ScheduleThreadFactory(executorName + "-initNewJob-thread" ,false));
//        treeCache.getListenable().addListener(new InitNewJobListener() ,executorService);
        treeCache.start();
    }


    public void shutdown() {
        try {
            if (treeCache != null) {
                treeCache.close();
            }
        } catch (Throwable t) {
            LOGGER.error( LogEvents.ExecutorEvent.INIT_OR_SHUTDOWN, t.toString(), t);
        }
        try {
            if (executorService != null && !executorService.isTerminated()) {
                executorService.shutdownNow();
                int count = 0;
                while (!executorService.awaitTermination(50, TimeUnit.MILLISECONDS)) {
                    if (++count == 4) {
                        LOGGER.info(LogEvents.ExecutorEvent.INIT_OR_SHUTDOWN,
                                "InitNewJob executorService try to shutdown now");
                        count = 0;
                    }
                    executorService.shutdownNow();
                }
            }
        } catch (Throwable t) {
            LOGGER.error( LogEvents.ExecutorEvent.INIT_OR_SHUTDOWN, t.toString(), t);
        }
    }

    public boolean removeJobName(String jobName){
        return jobNames.remove(jobName);
    }

//    class InitNewJobListener implements TreeCacheListener{
//
//        @Override
//        public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
//            if (treeCacheEvent== null){
//                return;
//            }
//            ChildData data = treeCacheEvent.getData();
//            if (data == null){
//                return;
//            }
//            String path = data.getPath();
//            if (path == null || path.equals(JobNodePath.ROOT)){
//                return;
//            }
//            TreeCacheEvent.Type type = treeCacheEvent.getType();
//
//            if (type == null || !type.equals(TreeCacheEvent.Type.NODE_ADDED)){
//                return;
//            }
//            String jobName = StringUtils.substringAfter(path , "/");
//            String jobClassPath = JobNodePath.getNodeFullPath(jobName , ConfigurationNode.JOB_CLASS);
//            for (int i =  0 ; i < WAIT_JOBCLASS_ADDED_COUNT; i ++ ){
//                if (!regCenter.isExisted(jobClassPath)){
//                    Thread.sleep(200L);
//                    continue;
//                }
//                LOGGER.info( jobName, "new job: {} 's jobClass created event received", jobName);
//
//                if (!jobNames.contains(jobName)) {
//                    if (canInitTheJob(jobName) && initJobScheduler(jobName)) {
//                        jobNames.add(jobName);
//                        LOGGER.info( jobName, "the job {} initialize successfully", jobName);
//                    }
//                } else {
//                    LOGGER.warn( jobName,
//                            "the job {} is unnecessary to initialize, because it's already existing", jobName);
//                }
//                break;
//            }
//        }
//
//        private boolean initJobScheduler(String jobName) {
//            try{
//                LOGGER.info( jobName, "start to initialize the new job");
//                JOB_INIT_FAILED_RECORDS.get(executorName).putIfAbsent(jobName, new HashSet<Integer>());
//                JobConfiguration jobConfig = new JobConfiguration(regCenter, jobName);
//                String jobTypeStr = jobConfig.getJobType();
//                JobType jobType = JobTypeManager.get(jobTypeStr);
//                if (jobType == null) {
//                    String message = String
//                            .format("the jobType %s is not supported by the executor version %s", jobTypeStr,
//                                    shrineExecutorService.getExecutorVersion());
//                    LOGGER.warn( jobName, message);
//                    throw new JobInitAlarmException(message);
//                }
//                if (jobType.getHandlerClass() == null) {
//                    throw new JobException(
//                            "unexpected error, the saturnJobClass cannot be null, jobName is %s, jobType is %s",
//                            jobName, jobTypeStr);
//                }
//                if (jobConfig.isDeleting()) {
//                    String serverNodePath = JobNodePath.getServerNodePath(jobName, executorName);
//                    regCenter.remove(serverNodePath);
//                    LOGGER.warn( jobName, "the job is on deleting");
//                    return false;
//                }
//                JobScheduler scheduler = new JobScheduler( jobConfig , regCenter);
//                scheduler.setShrineExecutorService(shrineExecutorService);
//                scheduler.init();
//                // clear previous records when initialize job successfully
//                JOB_INIT_FAILED_RECORDS.get(executorName).get(jobName).clear();
//                return true;
//            } catch (JobInitAlarmException e) {
//                if (!SystemEnvProperties.SHRINE_DISABLE_JOB_INIT_FAILED_ALARM) {
//                    // no need to log exception stack as it should be logged in the original happen place
//                    raiseAlarmForJobInitFailed(jobName, e);
//                }
//            }catch (Throwable t){
//                LOGGER.error(jobName , "job initialize failed, but will not stop the init process", t);
//            }
//            return false;
//        }
//
//        private void raiseAlarmForJobInitFailed(String jobName, JobInitAlarmException jobInitAlarmException) {
//            String message = jobInitAlarmException.getMessage();
//            int messageHashCode = message.hashCode();
//            Set<Integer> records = JOB_INIT_FAILED_RECORDS.get(executorName).get(jobName);
//            if (!records.contains(messageHashCode)) {
//                try {
//                    String namespace = regCenter.getNamespace();
//                    AlarmUtils.raiseAlarm(constructAlarmInfo(namespace, jobName, executorName, message), namespace);
//                    records.add(messageHashCode);
//                } catch (Exception e) {
//                    LOGGER.error( jobName, "exception throws during raise alarm for job init fail", e);
//                }
//            } else {
//                LOGGER.info( jobName,
//                        "job initialize failed but will not raise alarm as such kind of alarm already been raise before");
//            }
//        }
//        private Map<String, Object> constructAlarmInfo(String namespace, String jobName, String executorName,
//                                                       String alarmMessage) {
//            Map<String, Object> alarmInfo = new HashMap<>();
//
//            alarmInfo.put("jobName", jobName);
//            alarmInfo.put("executorName", executorName);
//            alarmInfo.put("name", "Saturn Event");
//            alarmInfo.put("title", String.format("JOB_INIT_FAIL:%s", jobName));
//            alarmInfo.put("level", "CRITICAL");
//            alarmInfo.put("message", alarmMessage);
//
//            Map<String, String> customFields = Maps.newHashMap();
//            customFields.put("sourceType", "saturn");
//            customFields.put("domain", namespace);
//            alarmInfo.put("additionalInfo", customFields);
//
//            return alarmInfo;
//        }
//
//        /**
//         * 如果Executor配置了Groups，则只能初始化属于groups的作业; 否则初始化全部作业
//         */
//        private boolean canInitTheJob(String jobName) {
//            Set<String> executorGroups = SystemEnvProperties.SHRINE_INIT_JOB_BY_GROUPS;
//            if (executorGroups.isEmpty()){
//                return true;
//            }
//            String jobGrops = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName , ConfigurationNode.GROUPS));
//            if (StringUtils.isNoneBlank(jobGrops)) {
//
//                String[] spilt = jobGrops.split(",");
//                for (String temp : spilt) {
//                    if (StringUtils.isBlank(temp)) {
//                        continue;
//                    }
//                    if (executorGroups.contains(temp.trim())) {
//                        return true;
//                    }
//                }
//            }
//                LOGGER.info( jobName, "the job {} wont be initialized, because it's not in the groups {}", jobName,
//                        executorGroups);
//                return false;
//        }
//    }
    public static boolean containsJobInitFailedRecord(String executorName, String jobName, String message) {
        return JOB_INIT_FAILED_RECORDS.get(executorName).get(jobName).contains(message.hashCode());
    }


}
