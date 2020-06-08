
package com.hailin.job.schedule.core.config.script;
import com.hailin.job.schedule.core.config.JobConfiguration;
import com.hailin.job.schedule.core.config.JobType;
import com.hailin.job.schedule.core.config.JobTypeConfiguration;
import com.hailin.job.schedule.core.job.type.script.ScriptJob;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 脚本作业配置.
 * 
 */
@RequiredArgsConstructor
@Getter
public final class ScriptJobConfiguration implements JobTypeConfiguration {
    
    private final JobConfiguration coreConfig;
    
    private final JobType jobType = JobType.SCRIPT;
    
    private final String jobClass = ScriptJob.class.getCanonicalName();
    
    private final String scriptCommandLine;
}
