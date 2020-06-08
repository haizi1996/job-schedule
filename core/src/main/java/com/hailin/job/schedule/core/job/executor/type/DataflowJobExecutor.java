
package com.hailin.job.schedule.core.job.executor.type;


import com.hailin.job.schedule.core.config.dataflow.DataflowJobConfiguration;
import com.hailin.job.schedule.core.executor.ShardingContext;
import com.hailin.job.schedule.core.job.JobFacade;
import com.hailin.job.schedule.core.job.type.dataflow.DataflowJob;
import com.hailin.job.schedule.core.job.executor.AbstractShrineJobExecutor;

import java.util.List;

/**
 * 数据流作业执行器.
 * 
 */
public final class DataflowJobExecutor extends AbstractShrineJobExecutor {
    
    private final DataflowJob<Object> dataflowJob;
    
    public DataflowJobExecutor(final DataflowJob<Object> dataflowJob, final JobFacade jobFacade) {
        super(jobFacade);
        this.dataflowJob = dataflowJob;
    }
    
    @Override
    protected void process(final ShardingContext shardingContext) {
        DataflowJobConfiguration dataflowConfig = (DataflowJobConfiguration) getJobRootConfig().getTypeConfig();
        if (dataflowConfig.isStreamingProcess()) {
            streamingExecute(shardingContext);
        } else {
            oneOffExecute(shardingContext);
        }
    }
    
    private void streamingExecute(final ShardingContext shardingContext) {
        List<Object> data = fetchData(shardingContext);
        while (null != data && !data.isEmpty()) {
            processData(shardingContext, data);
            if (!getJobFacade().isEligibleForJobRunning()) {
                break;
            }
            data = fetchData(shardingContext);
        }
    }
    
    private void oneOffExecute(final ShardingContext shardingContext) {
        List<Object> data = fetchData(shardingContext);
        if (null != data && !data.isEmpty()) {
            processData(shardingContext, data);
        }
    }
    
    private List<Object> fetchData(final ShardingContext shardingContext) {
        return dataflowJob.fetchData(shardingContext);
    }
    
    private void processData(final ShardingContext shardingContext, final List<Object> data) {
        dataflowJob.processData(shardingContext, data);
    }
}
