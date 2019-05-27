
package com.hailin.shrine.job.core.config.dataflow;


import com.hailin.shrine.job.core.config.JobConfiguration;
import com.hailin.shrine.job.core.config.JobType;
import com.hailin.shrine.job.core.config.JobTypeConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 数据流作业配置信息.
 * 
 */
@RequiredArgsConstructor
@Getter
public final class DataflowJobConfiguration implements JobTypeConfiguration {
    
    private final JobConfiguration coreConfig;
    
    private final JobType jobType = JobType.DATAFLOW;
    
    private final String jobClass;
    
    private final boolean streamingProcess;


}
