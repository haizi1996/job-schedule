
package com.hailin.shrine.job.core.job.type.dataflow;


import com.hailin.shrine.job.core.executor.ShardingContext;
import com.hailin.shrine.job.core.job.ElasticJob;

import java.util.List;

/**
 * 数据流分布式作业接口.
 * 
 * @author zhangliang
 * 
 * @param <T> 数据类型
 */
public interface DataflowJob<T> extends ElasticJob {
    
    /**
     * 获取待处理数据.
     *
     * @param shardingContext 分片上下文
     * @return 待处理的数据集合
     */
    List<T> fetchData(ShardingContext shardingContext);
    
    /**
     * 处理数据.
     *
     * @param shardingContext 分片上下文
     * @param data 待处理数据集合
     */
    void processData(ShardingContext shardingContext, List<T> data);
}
