package com.hailin.job.schedule.core.basic.control;

import com.google.common.collect.Maps;
import com.hailin.job.schedule.core.basic.execution.ExecutionInfo;
import com.hailin.job.schedule.core.basic.execution.ExecutionNode;
import com.hailin.job.schedule.core.basic.sharding.ShardingService;
import com.hailin.job.schedule.core.strategy.JobScheduler;
import com.hailin.shrine.job.common.util.LogEvents;
import com.hailin.job.schedule.core.basic.AbstractScheduleService;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ReportService extends AbstractScheduleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);

    private Map<Integer , ExecutionInfo> infoMap = Maps.newHashMap();

    public ReportService(JobScheduler jobScheduler) {
        super(jobScheduler);
    }

    /**
     * 将报告信息写入zk
     */
    public void reportDataToZK(){
        synchronized (infoMap){
            if(infoMap.size() == 0){
                return;
            }
            List<Integer> shardingItems = jobScheduler.getExecutionContextService().getShardingItems();

            for (Map.Entry<Integer , ExecutionInfo> entry : infoMap.entrySet()){
                Integer item = entry.getKey();
                ExecutionInfo info = entry.getValue();
                if(!shardingItems.contains(item)){
                    LOGGER.info( LogEvents.ExecutorEvent.COMMON,
                            "sharding items don't have such item: {}, reporter is going to ignore this "
                                    + "executionInfo: {}", item, info);
                    continue;
                }
                if (info.getLastBeginTime() != null){
                    getJobNodeStorage().replaceJobNode(ExecutionNode.getLastBeginTimeNode(item) , info.getLastBeginTime());
                }
                if (info.getLastCompleteTime() != null) {
                    getJobNodeStorage().replaceJobNode(ExecutionNode.getLastCompleteTimeNode(item),
                            info.getLastCompleteTime());
                }
                if (info.getNextFireTime() != null) {
                    getJobNodeStorage()
                            .replaceJobNode(ExecutionNode.getNextFireTimeNode(item), info.getNextFireTime());
                }
                getJobNodeStorage().replaceJobNode(ExecutionNode.getJobLog(item),
                        (info.getJobLog() == null ? "" : info.getJobLog()));
                getJobNodeStorage().replaceJobNode(ExecutionNode.getJobMsg(item),
                        (info.getJobMsg() == null ? "" : info.getJobMsg()));
                LOGGER.info( LogEvents.ExecutorEvent.COMMON, "done flushed {} to zk.", info);
            }
            infoMap.clear();
        }
    }

    public void clearInfoMap(){
        synchronized (infoMap){
            infoMap.clear();
        }
    }

    public void initInfoOnbegin(int item, Long nextFireTime) {
        synchronized (infoMap) {
            ExecutionInfo info = new ExecutionInfo(item, System.currentTimeMillis());
            if (nextFireTime != null) {
                info.setNextFireTime(nextFireTime);
            }
            infoMap.put(item , info);
        }
    }

    public ExecutionInfo getInfoByItem(int item) {
        synchronized (infoMap) {
            return infoMap.get(item);
        }
    }

    public void fillInfoOnAfter(ExecutionInfo info) {
        synchronized (infoMap) {
            infoMap.put(info.getItem(), info);
        }
    }

    public void updateExecutionInfoOnBefore() {
        synchronized (infoMap) {
            infoMap.clear();
        }
    }

    public Map<Integer, ExecutionInfo> getInfoMap() {
        return infoMap;
    }

    public void setInfoMap(Map<Integer, ExecutionInfo> infoMap) {
        this.infoMap = infoMap;
    }
}
