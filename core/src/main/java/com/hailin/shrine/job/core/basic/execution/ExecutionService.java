package com.hailin.shrine.job.core.basic.execution;

import com.google.common.collect.Lists;
import com.hailin.shrine.job.ShrineJobReturn;
import com.hailin.shrine.job.ShrineSystemErrorGroup;
import com.hailin.shrine.job.core.basic.AbstractShrineService;
import com.hailin.shrine.job.core.basic.ShrineExecutionContext;
import com.hailin.shrine.job.core.basic.control.ReportService;
import com.hailin.shrine.job.core.basic.failover.FailoverNode;
import com.hailin.shrine.job.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.shrine.job.core.reg.exception.RegException;
import com.hailin.shrine.job.core.service.ConfigurationService;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public ExecutionService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    @Override
    public void start() {
        configurationService = jobScheduler.getConfigService();
        reportService = jobScheduler.getReportService();
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
    public void registerJobBegin(){}

    /**
     * 注册作业完成信息.
     *
     */
    public void registerJobCompletedControlInfoByItem(
            final JobExecutionMultipleShardingContext jobExecutionShardingContext, int item) {

        boolean isEnabledReport = configurationService.isEnabledReport();
        if (!isEnabledReport) {
            return;
        }

        updateErrorJobReturnIfPossible(jobExecutionShardingContext, item);
        // create completed node
        createCompletedNode(item);
        // remove running node
        getJobNodeStorage().removeJobNode(ExecutionNode.getRunningNode(item));
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
    private boolean hasRunningItems(List<Integer> allItems) {
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





}
