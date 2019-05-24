
package com.hailin.shrine.job.core.job.executor.handler;


import com.hailin.shrine.job.common.util.ExecutorServiceObject;
import com.hailin.shrine.job.core.job.executor.ExecutorServiceHandler;

import java.util.concurrent.ExecutorService;

/**
 * 默认线程池服务处理器.
 * 
 */
public final class DefaultExecutorServiceHandler implements ExecutorServiceHandler {
    
    @Override
    public ExecutorService createExecutorService(final String jobName) {
        return new ExecutorServiceObject("inner-job-" + jobName, Runtime.getRuntime().availableProcessors() * 2).createExecutorService();
    }
}
