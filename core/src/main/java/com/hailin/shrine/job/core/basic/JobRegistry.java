package com.hailin.shrine.job.core.basic;

import com.google.common.collect.Maps;
import com.hailin.shrine.job.core.strategy.JobScheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 作业注册表
 * @author zhanghailin
 */
public class JobRegistry {

    private static volatile JobRegistry instance;

    //拼接符
    private static final String SEPARATOR = "@#@";

    private static Map<String , ConcurrentHashMap<String , JobScheduler>> schedulerMap = Maps.newConcurrentMap();

    private Map<String , Map<String , JobScheduler>>

    private JobRegistry(){}



    private Map<String , Integer> curentShardingTotalCountMap = Maps.newConcurrentMap();

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

    public static Map<String, ConcurrentHashMap<String, JobScheduler>> getSchedulerMap() {
        return schedulerMap;
    }

    public static void addJobScheduler(final String executorName , final String jobName
            , final JobScheduler jobScheduler ){
        schedulerMap.putIfAbsent(executorName ,  new ConcurrentHashMap<>()).put(jobName , jobScheduler);
    }

    public static void clearExecutor(String executorName){
        schedulerMap.remove(executorName);
    }

    public static void clearJob(String executorName , String jobName){
        schedulerMap.getOrDefault(executorName , new ConcurrentHashMap<>()).remove(jobName);
    }


}
