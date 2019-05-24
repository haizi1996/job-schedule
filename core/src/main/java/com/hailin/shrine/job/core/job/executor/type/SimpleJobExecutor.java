
package com.hailin.shrine.job.core.job.executor.type;

import com.hailin.shrine.job.core.executor.ShardingContext;
import com.hailin.shrine.job.core.job.JobFacade;
import com.hailin.shrine.job.core.job.executor.AbstractShrineJobExecutor;
import com.hailin.shrine.job.core.job.type.simple.SimpleJob;

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
