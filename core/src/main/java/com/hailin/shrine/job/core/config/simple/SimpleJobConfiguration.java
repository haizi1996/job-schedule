
package com.hailin.shrine.job.core.config.simple;


import com.hailin.shrine.job.core.config.JobConfiguration;
import com.hailin.shrine.job.core.config.JobType;
import com.hailin.shrine.job.core.config.JobTypeConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 简单作业配置.
 * 
 */
@RequiredArgsConstructor
@Getter
public final class SimpleJobConfiguration implements JobTypeConfiguration {
    
    private final JobConfiguration coreConfig;
    
    private final JobType jobType = JobType.SIMPLE;
    
    private final String jobClass;
}
