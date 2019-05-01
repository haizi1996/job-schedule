package com.hailin.hjob.common.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 作业核心配置项
 * @author zhanghailin
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class JobCoreConfiguration {

    /**
     * 作业名称
     */
    private final String jobName;

    /**
     * 作业cron表达式
     */
    private final String cron;

    /**
     * 作业分片总数量
     */
    private final int shardingTotalCount;

    /**
     * 作业分片项参数
     */
    private final String shardingItemParameters;

    /**
     * 作业参数
     */
    private final String jobParameter;

    /**
     * 是否开启作业执行失效转移。
     * 开启表示如果作业在一次作业执行中途宕机，
     * 允许将该次未完成的作业在另一作业节点上补偿执行。默认为 false
     */
    private final boolean failover;

    /**
     * 是否开启错过作业重新执行
     */
    private final boolean misfire;

    /**
     * 描述
     */
    private final String description;

    /**
     * 作业属性
     */
    private final JobProperties jobProperties;

    /**
     * 创建简单作业配置构建器.
     *
     * @param jobName 作业名称
     * @param cron 作业启动时间的cron表达式
     * @param shardingTotalCount 作业分片总数
     * @return 简单作业配置构建器
     */
    public static Builder newBuilder(final String jobName, final String cron, final int shardingTotalCount) {
        return new Builder(jobName, cron, shardingTotalCount);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {

        private final String jobName;

        private final String cron;

        private final int shardingTotalCount;

        private String shardingItemParameters = "";

        private String jobParameter = "";

        private boolean failover;

        private boolean misfire = true;

        private String description = "";

        private final JobProperties jobProperties = new JobProperties();

        /**
         * 设置分片序列号和个性化参数对照表.
         *
         * <p>
         * 分片序列号和参数用等号分隔, 多个键值对用逗号分隔. 类似map.
         * 分片序列号从0开始, 不可大于或等于作业分片总数.
         * 如:
         * 0=a,1=b,2=c
         * </p>
         *
         * @param shardingItemParameters 分片序列号和个性化参数对照表
         *
         * @return 作业配置构建器
         */
        public Builder shardingItemParameters(final String shardingItemParameters) {
            if (null != shardingItemParameters) {
                this.shardingItemParameters = shardingItemParameters;
            }
            return this;
        }

        /**
         * 设置作业自定义参数.
         *
         * <p>
         * 可以配置多个相同的作业, 但是用不同的参数作为不同的调度实例.
         * </p>
         *
         * @param jobParameter 作业自定义参数
         *
         * @return 作业配置构建器
         */
        public Builder jobParameter(final String jobParameter) {
            if (null != jobParameter) {
                this.jobParameter = jobParameter;
            }
            return this;
        }

        /**
         * 设置是否开启失效转移.
         *
         * <p>
         * 只有对monitorExecution的情况下才可以开启失效转移.
         * </p>
         *
         * @param failover 是否开启失效转移
         *
         * @return 作业配置构建器
         */
        public Builder failover(final boolean failover) {
            this.failover = failover;
            return this;
        }

        /**
         * 设置是否开启misfire.
         *
         * @param misfire 是否开启misfire
         *
         * @return 作业配置构建器
         */
        public Builder misfire(final boolean misfire) {
            this.misfire = misfire;
            return this;
        }

        /**
         * 设置作业描述信息.
         *
         * @param description 作业描述信息
         *
         * @return 作业配置构建器
         */
        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }

        /**
         * 设置作业属性.
         *
         * @param key 属性键
         * @param value 属性值
         *
         * @return 作业配置构建器
         */
        public Builder jobProperties(final String key, final String value) {
            jobProperties.put(key, value);
            return this;
        }

        /**
         * 构建作业配置对象.
         *
         * @return 作业配置对象
         */
        public final JobCoreConfiguration build() {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName), "jobName can not be empty.");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(cron), "cron can not be empty.");
            Preconditions.checkArgument(shardingTotalCount > 0, "shardingTotalCount should larger than zero.");
            return new JobCoreConfiguration(jobName, cron, shardingTotalCount, shardingItemParameters, jobParameter, failover, misfire, description, jobProperties);
        }
    }
}
