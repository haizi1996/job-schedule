
package com.hailin.job.schedule.core.basic.execution;


import com.hailin.shrine.job.common.exception.JobConfigurationException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.curator.shaded.com.google.common.base.Strings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 分片序列号个性化参数.
 *
 */
@Getter
public final class ShardingItemParameters {
    
    private static final String PARAMETER_DELIMITER = ",";
    
    private static final String KEY_VALUE_DELIMITER = "=";
    
    private final Map<Integer, String> map;
    
    public ShardingItemParameters(final String shardingItemParameters) {
        map = toMap(shardingItemParameters);
    }
    
    private Map<Integer, String> toMap(final String originalShardingItemParameters) {
        if (Strings.isNullOrEmpty(originalShardingItemParameters)) {
            return Collections.emptyMap();
        }
        String[] shardingItemParameters = originalShardingItemParameters.split(PARAMETER_DELIMITER);
        Map<Integer, String> result = new HashMap<>(shardingItemParameters.length);
        for (String each : shardingItemParameters) {
            ShardingItem shardingItem = parse(each, originalShardingItemParameters);
            result.put(shardingItem.item, shardingItem.parameter);
        }
        return result;
    }
    
    private ShardingItem parse(final String shardingItemParameter, final String originalShardingItemParameters) {
        String[] pair = shardingItemParameter.trim().split(KEY_VALUE_DELIMITER);
        if (2 != pair.length) {
            throw new JobConfigurationException("Sharding item parameters '%s' format error, should be int=xx,int=xx", originalShardingItemParameters);
        }
        try {
            return new ShardingItem(Integer.parseInt(pair[0].trim()), pair[1].trim());
        } catch (final NumberFormatException ex) {
            throw new JobConfigurationException("Sharding item parameters key '%s' is not an integer.", pair[0]);
        }
    }
    
    /**
     * 分片项.
     */
    @AllArgsConstructor
    private static final class ShardingItem {
        
        private final int item;
        
        private final String parameter;
    }
}