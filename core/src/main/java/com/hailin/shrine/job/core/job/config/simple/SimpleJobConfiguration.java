
package com.hailin.shrine.job.core.job.config.simple;


import com.hailin.shrine.job.core.job.config.JobCoreConfiguration;
import com.hailin.shrine.job.core.job.config.JobType;
import com.hailin.shrine.job.core.job.config.JobTypeConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 简单作业配置.
 * 
 */
@RequiredArgsConstructor
@Getter
public final class SimpleJobConfiguration implements JobTypeConfiguration {
    
    private final JobCoreConfiguration coreConfig;
    
    private final JobType jobType = JobType.SIMPLE;
    
    private final String jobClass;
}
