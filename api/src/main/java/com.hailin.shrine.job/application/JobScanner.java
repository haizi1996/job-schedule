package com.hailin.shrine.job.application;

import com.hailin.shrine.job.core.job.config.JobConfiguration;

import java.util.List;

/**
 * 扫描Job的扫描器
 * @author zhanghailin
 */
public interface JobScanner {

    /**
     * 扫描某个namespace下的job
     */
    List<JobConfiguration> scanner(String nameSpace);
}
