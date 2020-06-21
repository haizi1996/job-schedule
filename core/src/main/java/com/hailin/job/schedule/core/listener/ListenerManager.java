package com.hailin.job.schedule.core.listener;

import com.hailin.job.schedule.core.basic.analyse.AnalyseResetListenerManager;
import com.hailin.job.schedule.core.basic.config.ConfigurationListenerManager;
import com.hailin.job.schedule.core.basic.control.ControlListenerManager;
import com.hailin.job.schedule.core.basic.election.ElectionListenerManager;
import com.hailin.job.schedule.core.basic.failover.FailoverListenerManager;
import com.hailin.job.schedule.core.basic.server.JobOperationListenerManager;
import com.hailin.job.schedule.core.basic.sharding.ShardingListenerManager;
import com.hailin.job.schedule.core.strategy.JobScheduler;

/**
 * 作业注册的中心的监听器管理者
 * @author zhanghailin
 */
public class ListenerManager extends AbstractListenerManager {

    //主节点选举的监听管理
    private ElectionListenerManager electionListenerManager;

    //失败转移的监听管理
    private FailoverListenerManager failoverListenerManager;

    private JobOperationListenerManager jobOperationListenerManager;

    private ConfigurationListenerManager configurationListenerManager;

    private ShardingListenerManager shardingListenerManager;

    private AnalyseResetListenerManager analyseResetListenerManager;

    private ControlListenerManager controlListenerManager;

    public ListenerManager(final JobScheduler jobScheduler){
        super(jobScheduler);



    }

    public void start() {

        electionListenerManager = new ElectionListenerManager(jobScheduler);
        failoverListenerManager = new FailoverListenerManager(jobScheduler);
        jobOperationListenerManager = new JobOperationListenerManager(jobScheduler);
        configurationListenerManager = new ConfigurationListenerManager(jobScheduler);
        shardingListenerManager = new ShardingListenerManager(jobScheduler);
        analyseResetListenerManager = new AnalyseResetListenerManager(jobScheduler);
        controlListenerManager = new ControlListenerManager(jobScheduler);

        //开启监听 主服务器选举
        electionListenerManager.start();
        shardingListenerManager.start();
        // 故障转移的监听器
        failoverListenerManager.start();
        //作业 立即执行的管理器
        jobOperationListenerManager.start();
        // 配置监听器 监听cron, 下游, enabled
        configurationListenerManager.start();
        // 作业统计的监听器
        analyseResetListenerManager.start();
        controlListenerManager.start();
    }

    @Override
    public void shutdown() {
        electionListenerManager.shutdown();
        failoverListenerManager.shutdown();
        jobOperationListenerManager.shutdown();
        configurationListenerManager.shutdown();
        shardingListenerManager.shutdown();
        analyseResetListenerManager.shutdown();
        controlListenerManager.shutdown();
    }

}
