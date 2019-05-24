
package com.hailin.shrine.job.core.job.config.dataflow;


import com.hailin.shrine.job.core.job.config.JobCoreConfiguration;
import com.hailin.shrine.job.core.job.config.JobType;
import com.hailin.shrine.job.core.job.config.JobTypeConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 数据流作业配置信息.
 * 
 */
@RequiredArgsConstructor
@Getter
public final class DataflowJobConfiguration implements JobTypeConfiguration {
    
    private final JobCoreConfiguration coreConfig;
    
    private final JobType jobType = JobType.DATAFLOW;
    
    private final String jobClass;
    
    private final boolean streamingProcess;
}
