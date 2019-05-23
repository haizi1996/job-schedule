package com.hailin.shrine.job.core.service;


import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import com.hailin.shrine.job.common.exception.JobConfigurationException;
import com.hailin.shrine.job.common.exception.ShardingItemParametersException;
import com.hailin.shrine.job.common.util.GsonFactory;
import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.common.util.LogEvents;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.JobTypeManager;
import com.hailin.shrine.job.core.basic.threads.ShrineThreadFactory;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.job.config.JobType;
import com.hailin.shrine.job.core.job.constant.ConfigurationNode;
import com.hailin.shrine.job.core.job.constant.ShrineConstant;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import com.hailin.shrine.job.sharding.node.ShrineExecutorsNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 弹性化分布式作业配置服务.
 * @author zhanghailin
 */
public class ConfigurationService extends AbstractShrineService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationService.class);

    //双引用
    private static final String DOUBLE_QUOTE = "\"";

    private static final String PATTERN = ",(?=(([^\"]*\"){2})*[^\"]*$)";

    private TimeZone jobTimeZone;

    private ExecutorService executorService;

    public ConfigurationService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        super.start();
        executorService = Executors.newSingleThreadExecutor(new ShrineThreadFactory(executorName + "-" + jobName + "-enabledChanged", false));
    }

    @Override
    public void shutdown() {
        super.close();
        if (executorService != null){
            executorService.shutdown();
        }
    }

    public void notifyJobEnabledOrNot() {
        if (isJobEnabled()) {
            notifyJobEnabledIfNecessary();
        } else {
            notifyJobDisabled();
        }
    }

    /**
     * 如果是本地模式，并且配置了优先Executor，并且当前Executor不属于优先Executor，则不需要通知
     */
    public void notifyJobEnabledIfNecessary() {
        if (isLocalMode()) {
            List<String> preferList = getPreferList();
            if (preferList != null && !preferList.isEmpty() && !preferList.contains(executorName)) {
                return;
            }
        }
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    jobScheduler.getJob().notifyJobEnabled();
                } catch (Throwable t) {
                    LOGGER.error( LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
                }
            }
        });
    }

    public void notifyJobDisabled() {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    jobScheduler.getJob().notifyJobDisabled();
                } catch (Throwable t) {
                    LOGGER.error(LogEvents.ExecutorEvent.COMMON, t.getMessage(), t);
                }
            }
        });
    }
    /**
     * 该时间是否在作业暂停时间段范围内。
     * <p>
     * 特别的，无论pausePeriodDate，还是pausePeriodTime，如果解析发生异常，则忽略该节点，视为没有配置该日期或时分段。
     *
     * @param date 时间，本机时区的时间
     *
     * @return 该时间是否在作业暂停时间段范围内。
     */
    public boolean isInPausePeriod(Date date) {
        Calendar calendar = Calendar.getInstance(getTimeZone());
        calendar.setTime(date);
        int month = calendar.get(Calendar.MONTH) + 1; // Calendar.MONTH begin from 0.
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);

        boolean dateIn = false;
        String pausePeriodDate = jobConfiguration.getPausePeriodDate();
        boolean pausePeriodDateIsEmpty = (pausePeriodDate == null || pausePeriodDate.trim().isEmpty());
        if (!pausePeriodDateIsEmpty) {
            dateIn = checkIsInPausePeriodDateOrTime(month, day, "/", pausePeriodDate);
        }
        boolean timeIn = false;
        String pausePeriodTime = jobConfiguration.getPausePeriodTime();
        boolean pausePeriodTimeIsEmpty = (pausePeriodTime == null || pausePeriodTime.trim().isEmpty());
        if (!pausePeriodTimeIsEmpty) {
            timeIn = checkIsInPausePeriodDateOrTime(hour, minute, ":", pausePeriodTime);
        }

        if (pausePeriodDateIsEmpty) {
            if (pausePeriodTimeIsEmpty) {
                return false;
            } else {
                return timeIn;
            }
        } else {
            if (pausePeriodTimeIsEmpty) {
                return dateIn;
            } else {
                return dateIn && timeIn;
            }
        }
    }
    /**
     * 获取分片序列号和个性化参数对照表.<br>
     * 如果是本地模式的作业，则获取到[-1=xx]
     *
     * @return 分片序列号和个性化参数对照表
     */
    public Map<Integer, String> getShardingItemParameters() {
        Map<Integer, String> result = new HashMap<>();
        String value = jobConfiguration.getShardingItemParameters();
        if (Strings.isNullOrEmpty(value)) {
            return result;
        }
        // 解释命令行参数
        String[] shardingItemParameters = value.split(PATTERN);
        Map<String, String> result0 = new HashMap<>(shardingItemParameters.length);
        for (String each : shardingItemParameters) {
            String item = "";
            String exec = "";

            int index = each.indexOf('=');
            if (index > -1) {
                item = each.substring(0, index).trim();
                exec = each.substring(index + 1, each.length()).trim();
                // 去掉前后的双引号"
                if (exec.startsWith(DOUBLE_QUOTE)) {
                    exec = exec.substring(1);
                }

                if (exec.endsWith(DOUBLE_QUOTE)) {
                    exec = exec.substring(0, exec.length() - 1);
                }
            } else {
                throw new ShardingItemParametersException("Sharding item parameters '%s' format error", value);
            }
            result0.put(item, exec);
        }
        if (isLocalMode()) {
            if (result0.containsKey("*")) {
                result.put(-1, result0.get("*"));
            } else {
                throw new ShardingItemParametersException(
                        "Sharding item parameters '%s' format error with local mode job, should be *=xx", value);
            }
        } else {
            Iterator<Map.Entry<String, String>> iterator = result0.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                String item = next.getKey();
                String exec = next.getValue();
                try {
                    result.put(Integer.valueOf(item), exec);
                } catch (final NumberFormatException ex) {
                    LOGGER.warn( LogEvents.ExecutorEvent.COMMON,
                            "Sharding item key '%s' is invalid, it should be an integer, key '%s' will be dropped",
                            item, item, ex);
                }
            }
        }
        return result;
    }

    private boolean isLocalMode() {
        return jobConfiguration.isLocalMode();
    }

    private static boolean checkIsInPausePeriodDateOrTime(int h, int m, String splitChar,
                                                          String pausePeriodDateOrTime) {
        String[] periods = pausePeriodDateOrTime.split(",");

        boolean result = false;
        for (String period : periods) {
            String[] tmp = period.trim().split("-");
            if (tmp != null && tmp.length == 2) {
                String left = tmp[0].trim();
                String right = tmp[1].trim();
                String[] hmLeft = left.split(splitChar);
                String[] hmRight = right.split(splitChar);
                if (hmLeft != null && hmLeft.length == 2 && hmRight != null && hmRight.length == 2) {
                    try {
                        int hLeft = Integer.parseInt(hmLeft[0]);
                        int mLeft = Integer.parseInt(hmLeft[1]);
                        int hRight = Integer.parseInt(hmRight[0]);
                        int mRight = Integer.parseInt(hmRight[1]);
                        result = (h > hLeft || h == hLeft && m >= mLeft) // NOSONAR
                                && (h < hRight || h == hRight && m <= mRight); // NOSONAR
                        if (result) {
                            break;
                        }
                    } catch (NumberFormatException e) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return result;
    }
    /**
     * 获取是否开启失效转移.
     *
     * @return 是否开启失效转移
     */
    public boolean isFailover() {
        return jobConfiguration.isFailover();
    }

    /**
     * 获取是否开启作业.
     *
     * @return 作业是否开启
     */
    public boolean isJobEnabled() {
        return jobConfiguration.isEnabled();
    }

    /**
     * 获取统计作业处理数据数量的间隔时间.
     *
     * @return 统计作业处理数据数量的间隔时间
     */
    public int getProcessCountIntervalSeconds() {
        return jobConfiguration.getProcessCountIntervalSeconds();
    }

    /**
     * 本机当前时间是否在作业暂停时间段范围内。
     * <p>
     * 特别的，无论pausePeriodDate，还是pausePeriodTime，如果解析发生异常，则忽略该节点，视为没有配置该日期或时分段。
     *
     * @return 本机当前时间是否在作业暂停时间段范围内.
     */
    public boolean isInPausePeriod() {
        return isInPausePeriod(new Date());
    }
    /**
     * 获取作业时区对象
     */
    public TimeZone getTimeZone() {
        String timeZoneStr = jobConfiguration.getTimeZone();
        if (timeZoneStr == null || timeZoneStr.trim().isEmpty()) {
            timeZoneStr = ShrineConstant.TIME_ZONE_ID_DEFAULT;
        }
        if (jobTimeZone != null && timeZoneStr.equals(jobTimeZone.getID())) {
            return jobTimeZone;
        } else {
            jobTimeZone = TimeZone.getTimeZone(timeZoneStr);
            return jobTimeZone;
        }
    }



    /**
     *
     * 获取超时时间
     */
    public int getTimeoutSeconds(){
        return jobConfiguration.getTimeoutSeconds();
    }

    /**
     *
     * 获取是否显示正常日志
     */
    public boolean showNormalLog(){
        return jobConfiguration.isShowNormalLog();
    }

    public Map<String , String> getCustomContext(){
        String jobNodeData = getJobNodeStorage().getJobNodeData(ConfigurationNode.CUSTOM_CONTEXT);
        return toCustomContext(jobNodeData);
    }

    /**
     * 将str转为Map
     * @param customContextStr str字符串
     * @return 自定义上下文Map
     */
    private Map<String , String> toCustomContext(String customContextStr){
        Map<String ,String> customContext = JsonUtils.fromJson(customContextStr , new TypeToken<Map<String , String>>(){

        }.getType());
        if (customContext == null){
            customContext = Maps.newHashMap();
        }
        return customContext;
    }

    public JobType getJobType(){
        return JobTypeManager.get(jobConfiguration.getJobType());
    }

    /**
     * 获取作业启动时间的cron表达式.
     *
     * @return 作业启动时间的cron表达式
     */
    public String getCron() {
        return jobConfiguration.getCron();
    }
    public List<String> getPreferList(){
        List<String> result = Lists.newArrayList();
        String prefer = jobConfiguration.getPreferList();
        if (StringUtils.isBlank(prefer)){
            return result;
        }
        String[] executors = prefer.split(",");
        List<String> allExistingExecutors = getAllExistingExecutors();
        for (String executor : executors){
            executor = executor.trim();
            if (StringUtils.isNotEmpty(executor)){
                fillRealPreferListIfIsDockerOrNot(result , executor ,allExistingExecutors);
            }
        }
        return result;
    }
    /**
     * 如果prefer不是docker容器，并且preferList不包含，则直接添加；<br>
     * 如果prefer是docker容器（以@开头），则prefer为task，获取该task下的所有executor，如果不包含，添加进preferList。
     */
    private void fillRealPreferListIfIsDockerOrNot(List<String> preferList, String prefer,
                                                   List<String> allExistsExecutors) {
        if (!prefer.startsWith("@")) { // not docker server
            if (!preferList.contains(prefer)) {
                preferList.add(prefer);
            }
        } else { // docker server, get the real executorList by task
            fillRealPreferListOfContainer(preferList, prefer, allExistsExecutors);
        }
    }
    private void fillRealPreferListOfContainer(List<String> preferList, String prefer,
                                               List<String> allExistsExecutors) {
        String task = prefer.substring(1);
        for (int i = 0; i < allExistsExecutors.size(); i++) {
            String executor = allExistsExecutors.get(i);
            if (coordinatorRegistryCenter.isExisted(ShrineExecutorsNode.getExecutorTaskNodePath(executor))) {
                String taskData = coordinatorRegistryCenter.get(ShrineExecutorsNode.getExecutorTaskNodePath(executor));
                if (taskData != null && task.equals(taskData) && !preferList.contains(executor)) {
                    preferList.add(executor);
                }
            }
        }
    }

    private List<String> getAllExistingExecutors() {
        List<String> allExistsExecutors = new ArrayList<>();
        if (coordinatorRegistryCenter.isExisted(ShrineExecutorsNode.getExecutorsNodePath())) {
            List<String> executors = coordinatorRegistryCenter
                    .getChildrenKeys(ShrineExecutorsNode.getExecutorsNodePath());
            if (executors != null) {
                allExistsExecutors.addAll(executors);
            }
        }
        return allExistsExecutors;
    }


    /**
     * 获取作业是否上报状态。
     * 如果存在/config/enabledReport节点，则返回节点的内容；
     * 如果不存在/config/enabledReport节点，如果作业类型是Java或者Shell，则返回true；否则，返回false；
     */
    public boolean isEnabledReport() {
        Boolean isEnabledReportInJobConfig = jobConfiguration.getEnabledReport();

        if (isEnabledReportInJobConfig != null) {
            return isEnabledReportInJobConfig;
        }
        // cron和passive作业默认上报
        JobType jobType = JobTypeManager.get(jobConfiguration.getJobType());
        if (jobType.isCron() || jobType.isPassive()) {
            return true;
        }

        return false;
    }

    public boolean isUseDispreferList() {
        return jobConfiguration.isUseDispreferList();
    }

    /**
     *
     * 获取作业分片总数.
     *
     * @return 作业分片总数
     */
    public int getShardingTotalCount() {
        return jobConfiguration.getShardingTotalCount();
    }


    public List<String> getDownStream() {
        List<String> downStreamList = new ArrayList<>();
        String downStream = jobConfiguration.getDownStream();
        if (StringUtils.isBlank(downStream)) {
            return downStreamList;
        }
        String[] split = downStream.split(",");
        for (String childName : split) {
            String childNameTrim = childName.trim();
            if (!childNameTrim.isEmpty()) {
                downStreamList.add(childNameTrim);
            }
        }
        return downStreamList;
    }
    /**
     * 持久化分布式作业配置信息.
     *
     * @param liteJobConfig 作业配置
     */
    public void persist(final JobConfiguration liteJobConfig) {
        checkConflictJob(liteJobConfig);
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT) || liteJobConfig.isOverwrite()) {
            jobNodeStorage.replaceJobNode(ConfigurationNode.ROOT, GsonFactory.getGson().toJson(liteJobConfig));
        }
    }

    private void checkConflictJob(final JobConfiguration liteJobConfig) {
        Optional<JobConfiguration> liteJobConfigFromZk = find();
        if (liteJobConfigFromZk.isPresent() && !liteJobConfigFromZk.get().getTypeConfig().getJobClass().equals(liteJobConfig.getTypeConfig().getJobClass())) {
            throw new JobConfigurationException("Job conflict with register center. The job '%s' in register center's class is '%s', your job class is '%s'",
                    liteJobConfig.getJobName(), liteJobConfigFromZk.get().getTypeConfig().getJobClass(), liteJobConfig.getTypeConfig().getJobClass());
        }
    }

    private Optional<JobConfiguration> find() {
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT)) {
            return Optional.absent();
        }
        JobConfiguration result = GsonFactory.getGson().fromJson(jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT) , JobConfiguration.class);
        if (null == result) {
            // TODO 应该删除整个job node,并非仅仅删除config node
            jobNodeStorage.removeJobNodeIfExisted(ConfigurationNode.ROOT);
        }
        return Optional.fromNullable(result);
    }
}
