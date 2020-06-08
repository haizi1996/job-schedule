
package com.hailin.job.schedule.core.job.executor.type;

import com.hailin.job.schedule.core.executor.ShardingContext;
import com.hailin.job.schedule.core.job.JobFacade;
import com.hailin.job.schedule.core.job.type.simple.SimpleJob;
import com.hailin.job.schedule.core.job.executor.AbstractShrineJobExecutor;

/**
 * 简单作业执行器.
 * 
 */
public final class SimpleJobExecutor extends AbstractShrineJobExecutor {
    
    private final SimpleJob simpleJob;
    
    public SimpleJobExecutor(final SimpleJob simpleJob, final JobFacade jobFacade) {
        super(jobFacade);
        this.simpleJob = simpleJob;
    }
    
    @Override
    protected void process(final ShardingContext shardingContext) {
        simpleJob.execute(shardingContext);
    }
}
