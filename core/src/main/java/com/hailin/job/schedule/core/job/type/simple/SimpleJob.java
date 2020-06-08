package com.hailin.job.schedule.core.job.type.simple;


import com.hailin.job.schedule.core.executor.ShardingContext;
import com.hailin.job.schedule.core.job.ElasticJob;

/**
 * 简单分布式作业接口.
 * 
 * @author zhanghailin
 */
public interface SimpleJob extends ElasticJob {
    
    /**
     * 执行作业.
     *
     * @param shardingContext 分片上下文
     */
    void execute(ShardingContext shardingContext);
}
