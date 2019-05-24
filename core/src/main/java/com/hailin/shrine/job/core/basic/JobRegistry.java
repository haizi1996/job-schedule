package com.hailin.shrine.job.core.basic;

import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.schedule.JobScheduleController;
import com.hailin.shrine.job.core.strategy.JobInstance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 作业注册表
 * @author zhanghailin
 */
public class JobRegistry {

    private static volatile JobRegistry instance;


    private JobRegistry(){}


    /**
     * 作业调度器集合
     */
    private Map<String, JobScheduleController> schedulerMap = new ConcurrentHashMap<>();
    /**
     * 注册中心集合
     */
    private Map<String, CoordinatorRegistryCenter> regCenterMap = new ConcurrentHashMap<>();
    /**
     * 作业运行实例集合
     */
    private Map<String, JobInstance> jobInstanceMap = new ConcurrentHashMap<>();
    /**
     * 运行中作业集合
     * key：作业名字
     */
    private Map<String, Boolean> jobRunningMap = new ConcurrentHashMap<>();
    /**
     * 作业总分片数量集合
     * key：作业名字
     */
    private Map<String, Integer> currentShardingTotalCountMap = new ConcurrentHashMap<>();

    /**
     * 添加作业调度控制器.
     *
     * @param jobName 作业名称
     * @param jobScheduleController 作业调度控制器
     * @param regCenter 注册中心
     */
    public void registerJob(final String jobName, final JobScheduleController jobScheduleController, final CoordinatorRegistryCenter regCenter) {
        schedulerMap.put(jobName, jobScheduleController);
        regCenterMap.put(jobName, regCenter);
        regCenter.addCacheData("/" + jobName);
    }
    /**
     * 获取作业调度控制器.
     *
     * @param jobName 作业名称
     * @return 作业调度控制器
     */
    public JobScheduleController getJobScheduleController(final String jobName) {
        return schedulerMap.get(jobName);
    }

    /**
     * 获取作业注册表实例.
     *
     * @return 作业注册表实例
     */
    public static JobRegistry getInstance() {
        if (null == instance) {
            synchronized (JobRegistry.class) {
                if (null == instance) {
                    instance = new JobRegistry();
                }
            }
        }
        return instance;
    }
    /**
     * 设置作业是否在运行.
     *
     * @param jobName 作业名称
     * @param isRunning 作业是否在运行
     */
    public void setJobRunning(final String jobName, final boolean isRunning) {
        jobRunningMap.put(jobName, isRunning);
    }

    /**
     * 获取作业运行实例.
     *
     * @param jobName 作业名称
     * @return 作业运行实例
     */
    public JobInstance getJobInstance(final String jobName) {
        return jobInstanceMap.get(jobName);
    }

    /**
     * 设置当前分片总数.
     *
     * @param jobName 作业名称
     * @param currentShardingTotalCount 当前分片总数
     */
    public void setCurrentShardingTotalCount(final String jobName, final int currentShardingTotalCount) {
        currentShardingTotalCountMap.put(jobName, currentShardingTotalCount);
    }

    /**
     * 终止任务调度.
     *
     * @param jobName 作业名称
     */
    public void shutdown(final String jobName) {
        JobScheduleController scheduleController = schedulerMap.remove(jobName);
        if (null != scheduleController) {
            scheduleController.shutdown();
        }
        CoordinatorRegistryCenter regCenter = regCenterMap.remove(jobName);
        if (null != regCenter) {
            regCenter.evictCacheData("/" + jobName);
        }
        jobInstanceMap.remove(jobName);
        jobRunningMap.remove(jobName);
        currentShardingTotalCountMap.remove(jobName);
    }

    /**
     * 判断任务调度是否已终止.
     *
     * @param jobName 作业名称
     * @return 任务调度是否已终止
     */
    public boolean isShutdown(final String jobName) {
        return !schedulerMap.containsKey(jobName) || !jobInstanceMap.containsKey(jobName);
    }

}
