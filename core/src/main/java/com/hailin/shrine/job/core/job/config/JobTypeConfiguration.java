package com.hailin.shrine.job.core.job.config;

/**
 * 作业类型配置接口
 * @author zhanghailin
 */
public interface JobTypeConfiguration {
    /**
     * 获取作业类型.
     *
     * @return 作业类型
     */
    JobType getJobType();

    /**
     * 获取作业实现类名称.
     *
     * @return 作业实现类名称
     */
    String getJobClass();

    /**
     * 获取作业核心配置.
     *
     * @return 作业核心配置
     */
    JobConfiguration getCoreConfig();
}
