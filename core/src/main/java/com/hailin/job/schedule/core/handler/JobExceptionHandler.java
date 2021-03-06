package com.hailin.job.schedule.core.handler;

/**
 * 作业异常处理器
 * @author zhanghailin
 */
public interface JobExceptionHandler {

    /**
     * 处理作业异常
     * @param jobName 作业名
     * @param cause 异常原因
     */
    void handleException(String jobName, Throwable cause);
}
