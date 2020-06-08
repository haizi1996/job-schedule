package com.hailin.job.schedule.core.basic.sharding.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hailin.shrine.job.common.exception.JobException;
import com.hailin.job.schedule.core.job.trigger.Triggered;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * 作业运行时多分片上下文
 * @author zhanghailin
 */
public class JobExecutionMultipleShardingContext extends AbstractJobExecutionShardingContext{

    private static int initCollectionSize = 64;

    //运行在本作业服务器的分片序列号集合
    private List<Integer> shardingItems = Lists.newArrayList();

    //作业分片项的数据处理位置Map
    private Map<Integer , String> offsets = Maps.newHashMap();

    private Triggered triggered;

    public static int getInitCollectionSize() {
        return initCollectionSize;
    }

    /**
     * 根据分片项获取单分片作业运行时上下文.
     *
     * @param item 分片项
     * @return 单分片作业运行时上下文
     */
    public JobExecutionSingleShardingContext createJobExecutionSingleShardingContext(final int item) {
        JobExecutionSingleShardingContext result = new JobExecutionSingleShardingContext();
        try {
            BeanUtils.copyProperties(result, this);
        } catch (final IllegalAccessException | InvocationTargetException ex) {
            throw new JobException(ex);
        }
        result.setShardingItem(item);
        result.setShardingItemParameter(shardingItemParameters.get(item));
        result.setOffset(offsets.get(item));
        return result;
    }
    @Override
    public String toString() {
        return String
                .format("jobName: %s, shardingTotalCount: %s, shardingItems: %s, shardingItemParameters: %s, jobParameter: %s",
                        getJobName(), getShardingTotalCount(), shardingItems, shardingItemParameters,
                        getJobParameter());
    }

    public static void setInitCollectionSize(int initCollectionSize) {
        JobExecutionMultipleShardingContext.initCollectionSize = initCollectionSize;
    }

    public List<Integer> getShardingItems() {
        return shardingItems;
    }

    public void setShardingItems(List<Integer> shardingItems) {
        this.shardingItems = shardingItems;
    }

    public Map<Integer, String> getShardingItemParameters() {
        return shardingItemParameters;
    }

    public void setShardingItemParameters(Map<Integer, String> shardingItemParameters) {
        this.shardingItemParameters = shardingItemParameters;
    }

    public Map<Integer, String> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<Integer, String> offsets) {
        this.offsets = offsets;
    }

    public Triggered getTriggered() {
        return triggered;
    }

    public void setTriggered(Triggered triggered) {
        this.triggered = triggered;
    }
}
