package com.hailin.shrine.job.core.job.config;

import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;

/**
 * 作业配置信息
 * @author zhanghailin
 */
public class JobConfiguration {

    //作业名称
    private final String jobName;

    //注册中心
    private CoordinatorRegistryCenter coordinatorRegistryCenter;

    //作业的类
    private String jobClass = "";

    //作业分片总数
    private int shardingTotalCount;

    //
    private String timeZone;
}
