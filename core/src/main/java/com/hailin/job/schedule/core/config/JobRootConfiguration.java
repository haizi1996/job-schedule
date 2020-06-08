package com.hailin.job.schedule.core.config;

/**
 * 作业配置根接口
 * @author zhanghailin
 */
public interface JobRootConfiguration {

    /**
     * 获取作业类型配置.
     */
    JobTypeConfiguration getTypeConfig();
}
