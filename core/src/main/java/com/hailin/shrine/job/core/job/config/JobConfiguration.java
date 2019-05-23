package com.hailin.shrine.job.core.job.config;

import com.google.common.base.Strings;
import com.hailin.shrine.job.core.basic.storage.JobNodePath;
import com.hailin.shrine.job.core.job.constant.ConfigurationNode;
import com.hailin.shrine.job.core.job.constant.ShrineConstant;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import lombok.Getter;

/**
 * 作业核心配置项
 *
 * @author zhanghailin
 */
@Getter
public final class JobConfiguration implements JobRootConfiguration {

    /**
     * 作业名称
     */
    private String jobName;

    //注册中心
    private CoordinatorRegistryCenter regCenter = null;

    private String jobClass = "";

    /**
     * 作业cron表达式
     */
    private String cron;

    /**
     * 作业分片总数量
     */
    private int shardingTotalCount;

    /**
     * 作业分片项参数
     * 分片序列号和参数用等号分隔, 多个键值对用逗号分隔.
     * 类似map. 分片序列号从0开始, 不可大于或等于作业分片总数. 如: 0=a,1=b,2=c
     */
    private String shardingItemParameters;

    /**
     * 作业参数
     */
    private String jobParameter;

    /**
     * 是否开启作业执行失效转移。
     * 开启表示如果作业在一次作业执行中途宕机，
     * 允许将该次未完成的作业在另一作业节点上补偿执行。默认为 false
     */
    private boolean failover;

    /**
     * 是否开启错过作业重新执行
     */
    private boolean misfire;

    //时区
    private String timeZone;

    //作业暂停时间段 日期段
    private String pausePeriodDate = "";
    //作业暂停时间段 小时分钟段
    private String pausePeriodTime = "";


    //本地配置是否可覆盖注册中心配置 ，如果可以覆盖，每次启动作业都以本地配置为准
    private boolean overwrite;

    //作业描述信息
    private String description = "";
    /**
     * 作业属性
     */
    private JobProperties jobProperties;

    // 统计作业处理数据数量的时间间隔
    private int processCountIntervalSeconds = 300;

    //作业是否启用
    private boolean enabled;

    //Job超时时间
    private int timeoutSeconds;

    //默认不开启 ，只显示异常情况下的日志
    private boolean showNormalLog = false;

    //作业类型
    private String jobType;

    //作业接收的queue名字
    private String queueName = "";

    //执行作业发送的channel名字
    private String channelName = "";

    //每一个分片的权重
    private Integer loadLevel = 1;

    //每一个作业的预分配列表
    private String preferList = "";

    //是否上报执行信息 比如completed,running , timeout
    private Boolean enabledReport = null;

    //是否启用本地模式
    private boolean localMode = false;

    // 是否启用串行消费
    private boolean useSerial = false;

    //是否使用费prefeList
    private boolean useDispreferList = true;

    //下游作业
    private String downStream;

    private  JobTypeConfiguration typeConfig;

    public JobConfiguration(CoordinatorRegistryCenter regCenter,
                            String jobName) {
        this.jobName = jobName;
        this.regCenter = regCenter;
        reLoadConfig();
    }

    public JobConfiguration(String jobName) {
        this.jobName = jobName;
    }




    public void reLoadConfig(){
        if (regCenter == null){
            return;
        }
        String valStr = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName , ConfigurationNode.TIMEOUTSECONDS));
        if (!Strings.isNullOrEmpty(valStr)) {
            timeoutSeconds = Integer.parseInt(valStr);
        }

        jobClass = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.JOB_CLASS));
        if (jobClass != null) {
            jobClass = jobClass.trim();
        }
        jobType = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.JOB_TYPE));

        valStr = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.SHARDING_TOTAL_COUNT));
        if (!Strings.isNullOrEmpty(valStr)) {
            shardingTotalCount = Integer.parseInt(valStr);
        }

        timeZone = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.TIMEZONE));
        if (Strings.isNullOrEmpty(timeZone)) {
            timeZone = ShrineConstant.TIME_ZONE_ID_DEFAULT;
        }

        cron = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.CRON));
        pausePeriodDate = regCenter
                .getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.PAUSE_PERIOD_DATE));
        pausePeriodTime = regCenter
                .getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.PAUSE_PERIOD_TIME));
        shardingItemParameters = regCenter
                .getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.SHARDING_ITEM_PARAMETERS));
        jobParameter = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.JOB_PARAMETER));

        valStr = regCenter
                .getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.PROCESS_COUNT_INTERVAL_SECONDS));
        if (!Strings.isNullOrEmpty(valStr)) {
            processCountIntervalSeconds = Integer.parseInt(valStr);
        }

        failover = Boolean
                .parseBoolean(regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.FAILOVER)));
        enabled = Boolean
                .parseBoolean(regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.ENABLED)));
        description = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.DESCRIPTION));
        showNormalLog = Boolean.parseBoolean(
                regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.SHOW_NORMAL_LOG)));
        queueName = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.QUEUE_NAME));
        channelName = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.CHANNEL_NAME));

        valStr = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.LOAD_LEVEL));
        if (!Strings.isNullOrEmpty(valStr)) {
            loadLevel = Integer.valueOf(valStr);
        }

        preferList = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.PREFER_LIST));

        String enabledReportStr = regCenter
                .getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.ENABLED_REPORT));
        if (!Strings.isNullOrEmpty(enabledReportStr)) {
            enabledReport = Boolean.valueOf(enabledReportStr);
        }

        localMode = Boolean.parseBoolean(
                regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.LOCAL_MODE)));
        useSerial = Boolean.parseBoolean(
                regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.USE_SERIAL)));
        useDispreferList = Boolean.parseBoolean(
                regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.USE_DISPREFER_LIST)));
        downStream = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.DOWN_STREAM));
    }

    public boolean isDeleting(){
        return regCenter.isExisted(JobNodePath.getNodeFullPath(jobName, ConfigurationNode.TO_DELETE));
    }

    public String getCronFromZk(){
        cron = regCenter.getDirectly(JobNodePath.getNodeFullPath(jobName , null));
        return cron;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public CoordinatorRegistryCenter getRegCenter() {
        return regCenter;
    }

    public void setRegCenter(CoordinatorRegistryCenter regCenter) {
        this.regCenter = regCenter;
    }

    public String getJobClass() {
        return jobClass;
    }

    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public int getShardingTotalCount() {
        return shardingTotalCount;
    }

    public void setShardingTotalCount(int shardingTotalCount) {
        this.shardingTotalCount = shardingTotalCount;
    }

    public String getShardingItemParameters() {
        return shardingItemParameters;
    }

    public void setShardingItemParameters(String shardingItemParameters) {
        this.shardingItemParameters = shardingItemParameters;
    }

    public String getJobParameter() {
        return jobParameter;
    }

    public void setJobParameter(String jobParameter) {
        this.jobParameter = jobParameter;
    }

    public boolean isFailover() {
        return failover;
    }

    public void setFailover(boolean failover) {
        this.failover = failover;
    }

    public boolean isMisfire() {
        return misfire;
    }

    public void setMisfire(boolean misfire) {
        this.misfire = misfire;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getPausePeriodDate() {
        return pausePeriodDate;
    }

    public void setPausePeriodDate(String pausePeriodDate) {
        this.pausePeriodDate = pausePeriodDate;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public JobProperties getJobProperties() {
        return jobProperties;
    }

    public void setJobProperties(JobProperties jobProperties) {
        this.jobProperties = jobProperties;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public Integer getLoadLevel() {
        return loadLevel;
    }

    public void setLoadLevel(Integer loadLevel) {
        this.loadLevel = loadLevel;
    }

    public String getPreferList() {
        return preferList;
    }

    public void setPreferList(String preferList) {
        this.preferList = preferList;
    }

    public boolean isLocalMode() {
        return localMode;
    }

    public void setLocalMode(boolean localMode) {
        this.localMode = localMode;
    }

    public boolean isUseDispreferList() {
        return useDispreferList;
    }

    public void setUseDispreferList(boolean useDispreferList) {
        this.useDispreferList = useDispreferList;
    }

    public String getDownStream() {
        return downStream;
    }

    public void setDownStream(String downStream) {
        this.downStream = downStream;
    }

    public int getProcessCountIntervalSeconds() {
        return processCountIntervalSeconds;
    }

    public void setProcessCountIntervalSeconds(int processCountIntervalSeconds) {
        this.processCountIntervalSeconds = processCountIntervalSeconds;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public boolean isShowNormalLog() {
        return showNormalLog;
    }

    public void setShowNormalLog(boolean showNormalLog) {
        this.showNormalLog = showNormalLog;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public Boolean getEnabledReport() {
        return enabledReport;
    }

    public void setEnabledReport(Boolean enabledReport) {
        this.enabledReport = enabledReport;
    }

    public boolean isUseSerial() {
        return useSerial;
    }

    public void setUseSerial(boolean useSerial) {
        this.useSerial = useSerial;
    }

    public String getPausePeriodTime() {
        return pausePeriodTime;
    }

    public void setPausePeriodTime(String pausePeriodTime) {
        this.pausePeriodTime = pausePeriodTime;
    }
}
