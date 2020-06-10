package com.hailin.job.schedule.core.basic;

import com.hailin.job.schedule.core.schedule.JobScheduleController;
import com.hailin.job.schedule.core.strategy.JobInstance;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.job.schedule.core.strategy.JobScheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 作业注册表
 * @author zhanghailin
 */
public class JobRegistry {

    private static Map<String, ConcurrentHashMap<String, JobScheduler>> schedulerMap = new ConcurrentHashMap<>();

    private JobRegistry() {
    }

    public static Map<String, ConcurrentHashMap<String, JobScheduler>> getSchedulerMap() {
        return schedulerMap;
    }

    /**
     * 添加作业控制器.
     */
    public static void addJobScheduler(final String executorName, final String jobName,
                                       final JobScheduler jobScheduler) {
        if (schedulerMap.containsKey(executorName)) {
            schedulerMap.get(executorName).put(jobName, jobScheduler);
        } else {
            ConcurrentHashMap<String, JobScheduler> schedMap = new ConcurrentHashMap<>();
            schedMap.put(jobName, jobScheduler);
            schedulerMap.put(executorName, schedMap);
        }
    }

    public static void clearExecutor(String executorName) {
        schedulerMap.remove(executorName);
    }

    public static void clearJob(String executorName, String jobName) {
        Map<String, JobScheduler> scedMap = schedulerMap.get(executorName);
        if (scedMap != null) {
            scedMap.remove(jobName);
        }
    }
}
