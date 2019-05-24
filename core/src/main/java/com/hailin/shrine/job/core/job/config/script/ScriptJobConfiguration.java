
package com.hailin.shrine.job.core.job.config.script;


import com.hailin.shrine.job.core.job.config.JobCoreConfiguration;
import com.hailin.shrine.job.core.job.config.JobType;
import com.hailin.shrine.job.core.job.config.JobTypeConfiguration;
import com.hailin.shrine.job.core.job.type.script.ScriptJob;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 脚本作业配置.
 * 
 */
@RequiredArgsConstructor
@Getter
public final class ScriptJobConfiguration implements JobTypeConfiguration {
    
    private final JobCoreConfiguration coreConfig;
    
    private final JobType jobType = JobType.SCRIPT;
    
    private final String jobClass = ScriptJob.class.getCanonicalName();
    
    private final String scriptCommandLine;
}
