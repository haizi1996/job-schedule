package com.hailin.shrine.job.core.job.config;

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
