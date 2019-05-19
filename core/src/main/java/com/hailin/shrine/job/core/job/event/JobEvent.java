package com.hailin.shrine.job.core.job.event;

/**
 * 作业事件接口.
 */
public interface JobEvent {

    /**
     * 获取作业名称.
     *
     * @return 作业名称
     */
    String getJobName();
}
