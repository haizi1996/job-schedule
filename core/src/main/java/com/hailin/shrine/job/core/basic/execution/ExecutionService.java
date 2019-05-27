package com.hailin.shrine.job.core.basic.execution;

import com.google.common.collect.Lists;
import com.hailin.shrine.job.ShrineJobReturn;
import com.hailin.shrine.job.ShrineSystemErrorGroup;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.JobRegistry;
import com.hailin.shrine.job.core.basic.ShrineExecutionContext;
import com.hailin.shrine.job.core.basic.control.ReportService;
import com.hailin.shrine.job.core.basic.failover.FailoverNode;
import com.hailin.shrine.job.core.basic.sharding.ShardingNode;
import com.hailin.shrine.job.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;
import com.hailin.shrine.job.core.reg.exception.RegException;
import com.hailin.shrine.job.core.service.ConfigurationService;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 执行作业的服务
 * @author zhanghailin
 */
public class ExecutionService extends AbstractShrineService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorService.class);

    private ConfigurationService configurationService;

    private ReportService reportService;

    public ExecutionService(String jobName, CoordinatorRegistryCenter registryCenter) {
        super(jobName, registryCenter);
    }

    @Override
    public void start() {
//        configurationService = jobScheduler.getConfigService();
//        reportService = jobScheduler.getReportService();
    }

    /**
     * 更新当前作业服务器运行时分片的nextFireTime
     * @param shardingItems 分片集合
     */
    public void updateNextFireTime(final List<Integer> shardingItems){
        if (shardingItems.isEmpty()){
            return;
        }
        for (int item : shardingItems){
            updateNextFireTimeByItem(item);
        }
    }

    private void updateNextFireTimeByItem(int item) {

        if (jobScheduler == null){
            return;
        }
        Date nextFireTimePausePeriodEffected = jobScheduler.getNextFireTimePausePeriodEffected();
        if (null != nextFireTimePausePeriodEffected){{
        getJobNodeStorage().replaceJobNode(ExecutionNode.getNextFireTimeNode(item),
                nextFireTimePausePeriodEffected.getTime());}
        }
    }

    /**
     * 注册作业启动信息
     *
     */
    public void registerJobBegin(final ShardingContexts shardingContexts){
        JobRegistry.getInstance().setJobRunning(jobName, true);
        if (!configurationService.load(true).isMonitorExecution()) {
            return;
        }
        for (int each : shardingContexts.getShardingItemParameters().keySet()) {
            jobNodeStorage.fillEphemeralJobNode(ShardingNode.getRunningNode(each), "");
        }
    }
    /**
     * 注册作业完成信息.
     *
     * @param shardingContexts 分片上下文
     */
    public void registerJobCompleted(final ShardingContexts shardingContexts) {
        JobRegistry.getInstance().setJobRunning(jobName, false);
        if (!configurationService.load(true).isMonitorExecution()) {
            return;
        }
        for (int each : shardingContexts.getShardingItemParameters().keySet()) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getRunningNode(each));
        }
    }


    private void createCompletedNode(int item) {
        try {
            getJobNodeStorage().createOrUpdateJobNodeWithValue(ExecutionNode.getCompletedNode(item), executorName);
        } catch (RegException e) {
            LOGGER.warn( jobName, "update job complete node fail.", e);
        }
    }

    private void updateErrorJobReturnIfPossible(JobExecutionMultipleShardingContext jobExecutionShardingContext,
                                                int item) {
        if (jobExecutionShardingContext instanceof ShrineExecutionContext) {
            // 为了展现分片处理失败的状态
            ShrineExecutionContext shrineExecutionContext = (ShrineExecutionContext) jobExecutionShardingContext;
            if (!shrineExecutionContext.isSaturnJob()) {
                return;
            }

            ShrineJobReturn jobRet = shrineExecutionContext.getShardingItemResults().get(item);
            try {
                if (jobRet != null) {
                    int errorGroup = jobRet.getErrorGroup();
                    if (errorGroup == ShrineSystemErrorGroup.TIMEOUT) {
                        getJobNodeStorage().createJobNodeIfNeeded(ExecutionNode.getTimeoutNode(item));
                    } else if (errorGroup != ShrineSystemErrorGroup.SUCCESS) {
                        getJobNodeStorage().createJobNodeIfNeeded(ExecutionNode.getFailedNode(item));
                    }
                } else {
                    getJobNodeStorage().createJobNodeIfNeeded(ExecutionNode.getFailedNode(item));
                }
            } catch (RegException e) {
                LOGGER.warn( jobName, "update job return fail.", e);
            }
        }
    }

    /**
     * 清除任务被错过执行的标记.
     *
     * @param items 需要清除错过执行的任务分片项
     */
    public void clearMisfire(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getMisfireNode(each));
        }
    }
    /**
     * 清除分配分片序号的运行状态
     * @param items 需要清理的分片项列表
     */
    public void clearRunningInfo(final List<Integer> items) {
        items.forEach(item ->
                {
                    if (!getJobNodeStorage().isJobNodeExisted(FailoverNode.getExecutionFailoverNode(item))) {
                        getJobNodeStorage().removeJobNodeIfExisted(ExecutionNode.getRunningNode(item));
                        cleanShrineNode(item);
                    }
                }
        );
    }


    /**
     * 删除作业执行时信息.
     */
    public void removeExecutionInfo() {
        getJobNodeStorage().removeJobNodeIfExisted(ExecutionNode.ROOT);
    }
    /**
     * 清除全部分片的运行状态.
     */
    public void clearAllRunningInfo() {
        clearRunningInfo(getAllItems());
    }
    /**
     * 判断该分片是否已完成.
     *
     * @param item 运行中的分片路径
     * @return 该分片是否已完成
     */
    public boolean isCompleted(final int item) {
        return getJobNodeStorage().isJobNodeExisted(ExecutionNode.getCompletedNode(item));
    }

    public boolean isRunning(final int item) {
        return getJobNodeStorage().isJobNodeExisted(ExecutionNode.getRunningNode(item));
    }

    public boolean isFailover(final int item) {
        return getJobNodeStorage().isJobNodeExisted(FailoverNode.getExecutionFailoverNode(item));
    }


    /**
     * 判断是否还有执行中的作业
     * @return 是否还有执行中的作业
     */
    public boolean hasRunningItems(){
        return hasRunningItems(getAllItems());
    }

    /**
     * 判断分片项是否还有执行中的作业
     * @param allItems 需要判断的分片项列表
     */
    private boolean hasRunningItems(Collection<Integer> allItems) {
        return allItems.stream().anyMatch(item ->getJobNodeStorage().isJobNodeExisted(ExecutionNode.getRunningNode(item)));
    }


    private List<Integer> getAllItems(){
        return Lists.transform(
                getJobNodeStorage().getJobNodeChildrenKeys(ExecutionNode.ROOT)
        ,Integer::valueOf);
    }

    /**
     * 删除Shrine的作业item信息
     * @param item 作业分片
     */
    private void cleanShrineNode(int item){
        getJobNodeStorage().removeJobNodeIfExisted(ExecutionNode.getFailedNode(item));
        getJobNodeStorage().removeJobNodeIfExisted(ExecutionNode.getTimeoutNode(item));
    }


    /**
     * 设置任务被错过执行的标记.
     *
     * @param items 需要设置错过执行的任务分片项
     */
    public void setMisfire(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.getMisfireNode(each));
        }
    }
    /**
     * 如果当前分片项仍在运行则设置任务被错过执行的标记.
     *
     * @param items 需要设置错过执行的任务分片项
     * @return 是否错过本次执行
     */
    public boolean misfireIfHasRunningItems(final Collection<Integer> items) {
        if (!hasRunningItems(items)) {
            return false;
        }
        setMisfire(items);
        return true;
    }
    /**
     * 获取标记被错过执行的任务分片项.
     *
     * @param items 需要获取标记被错过执行的任务分片项
     * @return 标记被错过执行的任务分片项
     */
    public List<Integer> getMisfiredJobItems(final Collection<Integer> items) {
        List<Integer> result = new ArrayList<>(items.size());
        for (int each : items) {
            if (jobNodeStorage.isJobNodeExisted(ShardingNode.getMisfireNode(each))) {
                result.add(each);
            }
        }
        return result;
    }

    /**
     * 获取禁用的任务分片项.
     *
     * @param items 需要获取禁用的任务分片项
     * @return 禁用的任务分片项
     */
    public List<Integer> getDisabledItems(final List<Integer> items) {
        List<Integer> result = new ArrayList<>(items.size());
        for (int each : items) {
            if (jobNodeStorage.isJobNodeExisted(ShardingNode.getDisabledNode(each))) {
                result.add(each);
            }
        }
        return result;
    }
}
