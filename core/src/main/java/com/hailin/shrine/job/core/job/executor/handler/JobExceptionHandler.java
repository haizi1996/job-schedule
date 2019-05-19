package com.hailin.shrine.job.core.job.executor.handler;

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
    void handlerException(String jobName , Throwable cause);
}
